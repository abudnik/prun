/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2013 Andrey Budnik <budnik27@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

===========================================================================
*/

#include "scheduler.h"
#include "common/log.h"
#include "common/error_code.h"
#include "common/service_locator.h"
#include "job_manager.h"
#include "job_history.h"
#include "worker_manager.h"
#include "worker_command.h"
#include "statistics.h"

// hint: avoid deadlocks. always lock jobs mutex after workers mutex

namespace master {

Scheduler::Scheduler()
{
    jobs_.SetOnRemoveCallback( this, &Scheduler::OnRemoveJob );
}

void Scheduler::OnHostAppearance( WorkerPtr &worker )
{
    {
        std::unique_lock< std::mutex > lock( workersMut_ );
        nodeState_[ worker->GetIP() ].SetWorker( worker );
        typedef NodePriorityQueue::value_type value_type;
        nodePriority_.insert( value_type( worker->GetIP(), &nodeState_[ worker->GetIP() ] ) );
    }
    NotifyAll();
}

void Scheduler::DeleteWorker( const std::string &host )
{
    {
        std::unique_lock< std::mutex > lock( workersMut_ );

        for( auto it = nodeState_.begin(); it != nodeState_.end(); )
        {
            const NodeState &nodeState = it->second;
            const WorkerPtr &worker = nodeState.GetWorker();

            if ( worker->GetHost() != host )
            {
                ++it;
                continue;
            }

            const WorkerJob workerJob = worker->GetJob();

            StopWorker( worker->GetIP() );

            failedWorkers_.Add( workerJob, worker->GetIP() );

            nodePriority_.left.erase( worker->GetIP() );
            nodeState_.erase( it++ );

            // worker job should be rescheduled to any other node
            RescheduleJob( workerJob );
        }
    }
    NotifyAll();
}

void Scheduler::OnChangedWorkerState( std::vector< WorkerPtr > &workers )
{
    std::unique_lock< std::mutex > lock( workersMut_ );

    for( auto &worker : workers )
    {
        WorkerState state = worker->GetState();

        if ( state == WORKER_STATE_NOT_AVAIL )
        {
            auto it = nodeState_.find( worker->GetIP() );
            if ( it != nodeState_.end() )
            {
                NodeState &nodeState = it->second;
                if ( nodeState.GetNumBusyCPU() < 1 )
                    continue;

                const WorkerJob workerJob = worker->GetJob();

                PLOG( "Scheduler::OnChangedWorkerState: worker node is lost: nodeIP=" << worker->GetIP() <<
                      ", numExecutingTasks=" << workerJob.GetTotalNumTasks() );

                failedWorkers_.Add( workerJob, worker->GetIP() );
                nodeState.Reset();
                worker->ResetJob();
                UpdateNodePriority( worker->GetIP(), &nodeState );

                if ( RescheduleJob( workerJob ) )
                {
                    lock.unlock();
                    NotifyAll();
                    lock.lock();
                }
            }
            else
            {
                PLOG( "Scheduler::OnChangedWorkerState: scheduler doesn't know about worker"
                      " with ip = " << worker->GetIP() );
            }
        }
    }
}

void Scheduler::OnNewJob()
{
    if ( CanTakeNewJob() )
        PlanJobExecution();
}

void Scheduler::UpdateNodePriority( const std::string &ip, NodeState *nodeState )
{
    nodePriority_.left.erase( ip );
    if ( nodeState )
    {
        typedef NodePriorityQueue::value_type value_type;
        nodePriority_.insert( value_type( ip, nodeState ) );
    }
    else
    {
        PLOG_ERR( "Scheduler::UpdateNodePriority: nodeState is null, hostIP=" << ip );
    }
}

void Scheduler::PlanJobExecution()
{
    JobPtr job;

    IJobManager *jobManager = common::GetService< IJobManager >();
    if ( !jobManager->PopJob( job ) )
        return;

    const int numExec = GetNumPlannedExec( job );
    job->SetNumPlannedExec( numExec );

    const int64_t jobId = job->GetJobId();
    {
        std::unique_lock< std::mutex > lock( jobsMut_ );

        std::set< int > &tasks = tasksToSend_[ jobId ];
        for( int taskId = 0; taskId < numExec; ++taskId )
        {
            tasks.insert( taskId );
        }

        jobs_.Add( job, numExec );
    }

    PLOG_DBG( "Scheduler::PlanJobExecution: JobId=" << jobId << ", numExec=" << numExec );

    NotifyAll();
}

bool Scheduler::RescheduleJob( const WorkerJob &workerJob )
{
    bool found = false;
    std::set<int64_t> jobs;
    workerJob.GetJobs( jobs );

    std::unique_lock< std::mutex > lock( jobsMut_ );

    for( auto jobId : jobs )
    {
        JobPtr job;
        if ( jobs_.FindJobByJobId( jobId, job ) )
        {
            simultExecCnt_[ jobId ] -= workerJob.GetNumTasks( jobId );

            if ( job->GetMaxFailedNodes() >= 0 )
            {
                const size_t failedNodesCnt = failedWorkers_.GetFailedNodesCnt( jobId );
                if ( failedNodesCnt >= static_cast<size_t>( job->GetMaxFailedNodes() ) )
                {
                    StopWorkers( jobId );
                    jobs_.RemoveJob( jobId, false, "max failed nodes limit exceeded" );
                    continue;
                }
            }

            if ( job->IsNoReschedule() )
            {
                jobs_.DecrementJobExecution( jobId, workerJob.GetNumTasks( jobId ), false );
                continue;
            }

            WorkerJob::Tasks tasks;
            workerJob.GetTasks( jobId, tasks );
            for( auto taskId : tasks )
            {
                PLOG_DBG( "Scheduler::RescheduleJob: jobId=" << jobId << ", taskId=" << taskId );
                needReschedule_.emplace_back( jobId, taskId );
                found = true;
            }
        }
        else
        {
            PLOG( "Scheduler::RescheduleJob: Job for jobId=" << jobId << " not found" );
        }
    }
    return found;
}

bool Scheduler::GetReschedJobForWorker( const WorkerPtr &worker, WorkerJob &plannedJob, JobPtr &job, int numFreeCPU )
{
    int64_t jobId;
    bool foundReschedJob = false;

    for( auto it = needReschedule_.begin(); it != needReschedule_.end(); )
    {
        if ( plannedJob.GetTotalNumTasks() >= numFreeCPU )
            break;

        const WorkerTask &workerTask = *it;

        if ( !jobs_.FindJobByJobId( workerTask.GetJobId(), job ) ||
             !CanAddTaskToWorker( worker, plannedJob, workerTask.GetJobId(), job ) )
        {
            ++it;
            continue;
        }

        if ( foundReschedJob )
        {
            if ( workerTask.GetJobId() == jobId )
            {
                plannedJob.AddTask( jobId, workerTask.GetTaskId() );
                needReschedule_.erase( it++ );
                continue;
            }
        }
        else
        {
            jobId = workerTask.GetJobId();
            if ( !failedWorkers_.IsWorkerFailedJob( worker->GetIP(), jobId ) )
            {
                if ( job->IsHostPermitted( worker->GetHost() ) &&
                     job->IsGroupPermitted( worker->GetGroup() ) )
                {
                    foundReschedJob = true;
                    plannedJob.AddTask( jobId, workerTask.GetTaskId() );
                    plannedJob.SetExclusive( job->IsExclusive() );
                    history_.IncrementNumExec( jobId, worker->GetIP() );
                    needReschedule_.erase( it++ );
                    continue;
                }
            }
        }
        ++it;
    }

    return foundReschedJob;
}

bool Scheduler::GetJobForWorker( const WorkerPtr &worker, WorkerJob &plannedJob, JobPtr &job, int numFreeCPU )
{
    bool foundReschedJob = GetReschedJobForWorker( worker, plannedJob, job, numFreeCPU );
    int64_t jobId = plannedJob.GetJobId();

    for( auto j_it = jobs_.GetJobQueueBegin(); j_it != jobs_.GetJobQueueEnd(); ++j_it )
    {
        JobState &jobState = const_cast< JobState & >(j_it->first);
        if ( jobState.IsSendedCompletely() )
            continue;

        const JobPtr &j = jobState.GetJob();

        // plannedJob tasks must belong to the only one job,
        // so jobId must be the same
        if ( foundReschedJob && jobId != j->GetJobId() )
            continue;

        if ( failedWorkers_.IsWorkerFailedJob( worker->GetIP(), j->GetJobId() ) )
            continue;

        if ( !CanAddTaskToWorker( worker, plannedJob, j->GetJobId(), j ) )
            continue;

        auto it = tasksToSend_.find( j->GetJobId() );
        if ( it == tasksToSend_.end() )
            continue;

        std::set< int > &tasks = it->second;
        if ( !tasks.empty() )
        {
            if ( !j->IsHostPermitted( worker->GetHost() ) ||
                 !j->IsGroupPermitted( worker->GetGroup() ) )
                continue;

            for( auto it_task = tasks.begin(); it_task != tasks.end(); )
            {
                if ( plannedJob.GetTotalNumTasks() >= numFreeCPU ||
                     !CanAddTaskToWorker( worker, plannedJob, j->GetJobId(), j ) )
                    break;

                int taskId = *it_task;
                plannedJob.AddTask( j->GetJobId(), taskId );
                plannedJob.SetExclusive( j->IsExclusive() );
                history_.IncrementNumExec( jobId, worker->GetIP() );

                tasks.erase( it_task++ );
                if ( tasks.empty() )
                {
                    jobState.SetSendedCompletely( true );
                    tasksToSend_.erase( it );
                    break;
                }
            }
            job = const_cast<JobPtr &>( j );
            break;
        }
    }

    return plannedJob.GetTotalNumTasks() > 0;
}

bool Scheduler::GetTaskToSend( WorkerJob &workerJob, std::string &hostIP, JobPtr &job )
{
    std::unique_lock< std::mutex > lock_w( workersMut_ );

    auto it = nodePriority_.right.rbegin();

    std::unique_lock< std::mutex > lock_j( jobsMut_ );

    for( ; it != nodePriority_.right.rend(); ++it )
    {
        NodeState &nodeState = *(it->first);
        const int numFreeCPU = nodeState.GetNumFreeCPU();
        if ( numFreeCPU <= 0 )
            continue;

        WorkerPtr &w = nodeState.GetWorker();
        if ( !w->IsAvailable() )
            continue;

        if ( GetJobForWorker( w, workerJob, job, numFreeCPU ) )
        {
            w->GetJob() += workerJob;
            hostIP = w->GetIP();

            const int numTasks = workerJob.GetTotalNumTasks();
            nodeState.AllocCPU( numTasks );
            simultExecCnt_[ workerJob.GetJobId() ] += numTasks;

            PLOG_DBG( "Scheduler::GetTaskToSend: jobId=" << workerJob.GetJobId() <<
                      ", numTasks=" << numTasks << ", host=" << w->GetHost() << ", ip=" << hostIP <<
                      ", freeCPU=" << numFreeCPU << ", totalCPU=" << w->GetNumCPU() <<
                      ", memory=" << w->GetMemorySize() );
            return true;
        }
    }

    // if there is any worker available, but all queued jobs are
    // sended to workers, then take next job from job mgr queue
    lock_j.unlock();
    lock_w.unlock();
    PlanJobExecution();

    return false;
}

void Scheduler::OnTaskSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP )
{
    if ( !success )
    {
        {
            WorkerPtr w;
            IWorkerManager *workerManager = common::GetService< IWorkerManager >();
            if ( !workerManager->GetWorkerByIP( hostIP, w ) )
                return;

            PLOG( "Scheduler::OnTaskSendCompletion: job sending failed:"
                  " jobId=" << workerJob.GetJobId() << ", ip=" << hostIP );

            std::unique_lock< std::mutex > lock( workersMut_ );
            {
                JobPtr j;
                std::unique_lock< std::mutex > lock_j( jobsMut_ );
                if ( !jobs_.FindJobByJobId( workerJob.GetJobId(), j ) )
                    return;
            }

            auto it = nodeState_.find( hostIP );
            if ( it == nodeState_.end() )
                return;

            if ( failedWorkers_.Add( workerJob.GetJobId(), hostIP ) )
            {
                const int numTasks = workerJob.GetTotalNumTasks();
                NodeState &nodeState = it->second;
                nodeState.FreeCPU( numTasks );
                UpdateNodePriority( hostIP, &nodeState );

                // worker job should be rescheduled to any other node
                RescheduleJob( workerJob );
                WorkerJob &workerJob = w->GetJob();
                workerJob.DeleteJob( workerJob.GetJobId() );
            }
            else
            {
                PLOG_WRN( "Scheduler::OnTaskSendCompletion: job already sended" <<
                          ", jobId=" << workerJob.GetJobId() << ", ip=" << hostIP );
                return;
            }
        }
        NotifyAll();
    }
}

void Scheduler::OnTaskCompletion( int errCode, int64_t execTime, const WorkerTask &workerTask, const std::string &hostIP )
{
    if ( !errCode )
    {
        WorkerPtr w;
        IWorkerManager *workerManager = common::GetService< IWorkerManager >();
        if ( !workerManager->GetWorkerByIP( hostIP, w ) )
            return;

        std::unique_lock< std::mutex > lock_w( workersMut_ );
        std::unique_lock< std::mutex > lock_j( jobsMut_ );

        {
            JobPtr j;
            if ( !jobs_.FindJobByJobId( workerTask.GetJobId(), j ) )
                return;
        }

        WorkerJob &workerJob = w->GetJob();
        if ( !workerJob.DeleteTask( workerTask.GetJobId(), workerTask.GetTaskId() ) )
        {
            // task already processed.
            // it happens when a few threads simultaneously get success errCode from the same task
            // or after timeout
            return;
        }

        auto it = nodeState_.find( hostIP );
        if ( it == nodeState_.end() )
            return;

        NodeState &nodeState = it->second;
        nodeState.FreeCPU( 1 );
        UpdateNodePriority( hostIP, &nodeState );
        simultExecCnt_[ workerTask.GetJobId() ] -= 1;

        PLOG( "Scheduler::OnTaskCompletion: jobId=" << workerTask.GetJobId() <<
              ", taskId=" << workerTask.GetTaskId() << ", execTime=" << execTime << " ms"
              ", ip=" << hostIP );

        jobs_.DecrementJobExecution( workerTask.GetJobId(), 1, true );
    }
    else
    {
        if ( errCode == NODE_JOB_COMPLETION_NOT_FOUND )
            return;

        WorkerPtr w;
        IWorkerManager *workerManager = common::GetService< IWorkerManager >();
        if ( !workerManager->GetWorkerByIP( hostIP, w ) )
            return;

        std::unique_lock< std::mutex > lock_w( workersMut_ );
        {
            JobPtr j;
            std::unique_lock< std::mutex > lock_j( jobsMut_ );
            if ( !jobs_.FindJobByJobId( workerTask.GetJobId(), j ) )
                return;
        }

        auto it = nodeState_.find( hostIP );
        if ( it == nodeState_.end() )
            return;

        PLOG( "Scheduler::OnTaskCompletion: errCode=" << errCode <<
              ", jobId=" << workerTask.GetJobId() <<
              ", taskId=" << workerTask.GetTaskId() << ", ip=" << hostIP );

        if ( failedWorkers_.Add( workerTask.GetJobId(), hostIP ) )
        {
            WorkerJob jobToReschedule;
            jobToReschedule.AddTask( workerTask.GetJobId(), workerTask.GetTaskId() );

            NodeState &nodeState = it->second;
            nodeState.FreeCPU( 1 );
            UpdateNodePriority( hostIP, &nodeState );

            // worker task should be rescheduled to any other node
            RescheduleJob( jobToReschedule );
            WorkerJob &workerJob = w->GetJob();
            workerJob.DeleteTask( workerTask.GetJobId(), workerTask.GetTaskId() );
        }
        else
        {
            PLOG_WRN( "Scheduler::OnTaskCompletion: job already completed" <<
                      ", jobId=" << workerTask.GetJobId() <<
                      ", taskId=" << workerTask.GetTaskId() << ", ip=" << hostIP );
            return;
        }
    }

    NotifyAll();
}

void Scheduler::OnTaskTimeout( const WorkerTask &workerTask, const std::string &hostIP )
{
    WorkerPtr w;
    IWorkerManager *workerManager = common::GetService< IWorkerManager >();
    if ( !workerManager->GetWorkerByIP( hostIP, w ) )
        return;
    const WorkerJob &workerJob = w->GetJob();

    bool hasTask = false;
    {
        std::unique_lock< std::mutex > lock_w( workersMut_ );
        hasTask = workerJob.HasTask( workerTask.GetJobId(), workerTask.GetTaskId() );
    }

    if ( hasTask )
    {
        PLOG( "Scheduler::OnTaskTimeout " << workerTask.GetJobId() << ':' << workerTask.GetTaskId() << ' ' << hostIP );

        // send stop command to worker
        StopTaskCommand *stopCommand = new StopTaskCommand();
        stopCommand->SetParam( "job_id", workerTask.GetJobId() );
        stopCommand->SetParam( "task_id", workerTask.GetTaskId() );
        CommandPtr commandPtr( stopCommand );
        workerManager->AddCommand( commandPtr, hostIP );

        OnTaskCompletion( NODE_JOB_TIMEOUT, 0, workerTask, hostIP );
    }
}

void Scheduler::OnJobTimeout( int64_t jobId )
{
    {
        std::unique_lock< std::mutex > lock_w( workersMut_ );
        std::unique_lock< std::mutex > lock_j( jobsMut_ );

        {
            JobPtr j;
            if ( !jobs_.FindJobByJobId( jobId, j ) )
                return;
        }
        StopWorkers( jobId );
        jobs_.RemoveJob( jobId, false, "timeout" );
    }
    NotifyAll();
}

void Scheduler::StopJob( int64_t jobId )
{
    OnJobTimeout( jobId );
}

void Scheduler::StopJobGroup( int64_t groupId )
{
    std::list< JobPtr > jobs;
    {
        std::unique_lock< std::mutex > lock_j( jobsMut_ );
        jobs_.GetJobGroup( groupId, jobs );
    }
    for( const auto &job : jobs )
    {
        StopJob( job->GetJobId() );
    }
}

void Scheduler::StopNamedJob( const std::string &name )
{
    std::set< int64_t > jobs;
    {
        std::unique_lock< std::mutex > lock_j( jobsMut_ );
        jobs_.GetJobsByName( name, jobs );
    }
    for( auto jobId : jobs )
    {
        StopJob( jobId );
    }
}

void Scheduler::StopAllJobs()
{
    std::vector< int64_t > jobs;
    {
        std::unique_lock< std::mutex > lock_j( jobsMut_ );

        for( auto it = jobs_.GetJobQueueBegin(); it != jobs_.GetJobQueueEnd(); ++it )
        {
            const int64_t jobId = it->second;
            jobs.push_back( jobId );
        }
    }
    for( int64_t jobId : jobs )
    {
        StopJob( jobId );
    }

    // send stop all command
    {
        IWorkerManager *workerManager = common::GetService< IWorkerManager >();

        std::unique_lock< std::mutex > lock_w( workersMut_ );
        for( auto it = nodeState_.cbegin(); it != nodeState_.cend(); ++it )
        {
            const NodeState &nodeState = it->second;
            const WorkerPtr &worker = nodeState.GetWorker();

            Command *stopCommand = new StopAllJobsCommand();
            CommandPtr commandPtr( stopCommand );
            workerManager->AddCommand( commandPtr, worker->GetIP() );
        }
    }
}

void Scheduler::StopPreviousJobs()
{
    IWorkerManager *workerManager = common::GetService< IWorkerManager >();

    std::unique_lock< std::mutex > lock_w( workersMut_ );
    for( auto it = nodeState_.cbegin(); it != nodeState_.cend(); ++it )
    {
        const NodeState &nodeState = it->second;
        const WorkerPtr &worker = nodeState.GetWorker();

        Command *stopCommand = new StopPreviousJobsCommand();
        CommandPtr commandPtr( stopCommand );
        workerManager->AddCommand( commandPtr, worker->GetIP() );
    }
}

void Scheduler::OnRemoveJob( int64_t jobId, const std::string &jobName, bool success )
{
    simultExecCnt_.erase( jobId );
    history_.RemoveJob( jobId );
    failedWorkers_.Delete( jobId );

    IJobEventReceiver *jobEventReceiver = common::GetService< IJobEventReceiver >();
    jobEventReceiver->OnJobDelete( jobId, jobName );
}

void Scheduler::StopWorkers( int64_t jobId )
{
    {
        IWorkerManager *workerManager = common::GetService< IWorkerManager >();

        for( auto it = nodeState_.begin(); it != nodeState_.end(); ++it )
        {
            NodeState &nodeState = it->second;
            WorkerPtr &worker = nodeState.GetWorker();
            WorkerJob &workerJob = worker->GetJob();

            if ( workerJob.HasJob( jobId ) )
            {
                // send stop command to worker
                WorkerJob::Tasks tasks;
                workerJob.GetTasks( jobId, tasks );
                for( auto taskId : tasks )
                {
                    StopTaskCommand *stopCommand = new StopTaskCommand();
                    stopCommand->SetParam( "job_id", jobId );
                    stopCommand->SetParam( "task_id", taskId );
                    CommandPtr commandPtr( stopCommand );
                    workerManager->AddCommand( commandPtr, worker->GetIP() );
                }

                const int numTasks = workerJob.GetNumTasks( jobId );
                nodeState.FreeCPU( numTasks );
                workerJob.DeleteJob( jobId );
            }
        }
    }

    tasksToSend_.erase( jobId );
    {
        for( auto it = needReschedule_.begin(); it != needReschedule_.end(); )
        {
            if ( it->GetJobId() == jobId )
            {
                needReschedule_.erase( it++ );
                continue;
            }
            ++it;
        }
    }
}

void Scheduler::StopWorker( const std::string &hostIP ) const
{
    auto it = nodeState_.find( hostIP );
    if ( it == nodeState_.end() )
        return;

    const NodeState &nodeState = it->second;
    const WorkerPtr &worker = nodeState.GetWorker();
    const WorkerJob &workerJob = worker->GetJob();

    IWorkerManager *workerManager = common::GetService< IWorkerManager >();

    std::vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    for( const auto &workerTask : tasks )
    {
        StopTaskCommand *stopCommand = new StopTaskCommand();
        stopCommand->SetParam( "job_id", workerTask.GetJobId() );
        stopCommand->SetParam( "task_id", workerTask.GetTaskId() );
        CommandPtr commandPtr( stopCommand );
        workerManager->AddCommand( commandPtr, worker->GetIP() );
    }
}

bool Scheduler::CanTakeNewJob()
{
    std::unique_lock< std::mutex > lock_w( workersMut_ );

    auto it = nodePriority_.right.rbegin();
    if ( it != nodePriority_.right.rend() )
    {
        const NodeState &nodeState = *(it->first);
        return nodeState.GetNumFreeCPU() > 0;
    }

    return false;
}

bool Scheduler::CanAddTaskToWorker( const WorkerPtr &worker, const WorkerJob &workerPlannedJob,
                                    int64_t jobId, const JobPtr &job ) const
{
    const WorkerJob &workerJob = worker->GetJob();

    // job exclusive case
    if ( job->IsExclusive() || workerJob.IsExclusive() )
    {
        if ( workerJob.GetNumJobs() > 1 )
            return false;

        const int64_t id = workerJob.GetJobId();
        if ( id != -1 && id != jobId )
            return false;
    }

    // max instances of simultaneously running tasks per host case
    const int maxWorkerInstances = job->GetMaxWorkerInstances();
    if ( maxWorkerInstances > 0 )
    {
        const int numTasks = workerJob.GetNumTasks( jobId ) + workerPlannedJob.GetNumTasks( jobId );
        if ( numTasks >= maxWorkerInstances )
            return false;
    }

    // max cluster instances limit case
    if ( job->GetMaxClusterInstances() > 0 )
    {
        auto it = simultExecCnt_.find( jobId );
        if ( it != simultExecCnt_.end() )
        {
            const int numClusterInstances = (*it).second + workerPlannedJob.GetNumTasks( jobId );
            if ( numClusterInstances >= job->GetMaxClusterInstances() )
                return false;
        }
    }

    if ( job->GetMaxExecAtWorker() > 0 )
    {
        if ( history_.GetNumExec( jobId, worker->GetIP() ) >= job->GetMaxExecAtWorker() )
            return false;

        const int numTasks = workerJob.GetNumTasks( jobId ) + workerPlannedJob.GetNumTasks( jobId );
        if ( numTasks >= job->GetMaxExecAtWorker() )
            return false;
    }

    return true;
}

int Scheduler::GetNumPlannedExec( const JobPtr &job ) const
{
    if ( job->GetNumExec() > 0 )
        return job->GetNumExec();

    IWorkerManager *workerManager = common::GetService< IWorkerManager >();

    int numExec;
    if ( job->GetExecUnitType() == ExecUnitType::CPU )
    {
        numExec = workerManager->GetTotalCPU();
    }
    else // ExecUnitType::HOST
    {
        const size_t numHosts = job->GetNumPermittedHosts();
        if ( !numHosts )
        {
            numExec = workerManager->GetTotalWorkers();
        }
        else
        {
            numExec = numHosts;
        }
    }
    if ( numExec < 1 )
        numExec = 1;
    return numExec;
}

void Scheduler::Accept( ISchedulerVisitor *visitor )
{
    std::unique_lock< std::mutex > lock_w( workersMut_ );
    std::unique_lock< std::mutex > lock_j( jobsMut_ );

    visitor->Visit( *this );
}

} // namespace master

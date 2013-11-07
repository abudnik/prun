#include "scheduler.h"
#include "common/log.h"
#include "common/error_code.h"
#include "job_manager.h"
#include "worker_manager.h"
#include "worker_command.h"

// hint: avoid deadlocks. always lock jobs mutex after workers mutex

namespace master {

Scheduler::Scheduler()
{
    jobs_.SetOnRemoveCallback( this, &Scheduler::OnRemoveJob );
}

void Scheduler::OnHostAppearance( Worker *worker )
{
    {
        boost::mutex::scoped_lock scoped_lock( workersMut_ );
        nodeState_[ worker->GetIP() ].SetWorker( worker );
    }
    NotifyAll();
}

void Scheduler::OnChangedWorkerState( const std::vector< Worker * > &workers )
{
    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    std::vector< Worker * >::const_iterator it = workers.begin();
    for( ; it != workers.end(); ++it )
    {
        Worker *worker = *it;
        WorkerState state = worker->GetState();

        if ( state == WORKER_STATE_NOT_AVAIL )
        {
            IPToNodeState::iterator it = nodeState_.find( worker->GetIP() );
            if ( it != nodeState_.end() )
            {
                NodeState &nodeState = it->second;
                if ( nodeState.GetNumBusyCPU() < 1 )
                    continue;

                const WorkerJob workerJob = worker->GetJob();

                PS_LOG( "Scheduler::OnChangedWorkerState: worker isn't available, while executing job"
                        "; nodeIP=" << worker->GetIP() << ", numTasks=" << workerJob.GetTotalNumTasks() );

                failedWorkers_.Add( workerJob, worker->GetIP() );
                nodeState.Reset();
                worker->ResetJob();

                if ( RescheduleJob( workerJob ) )
                {
                    scoped_lock.unlock();
                    NotifyAll();
                    scoped_lock.lock();
                }
            }
            else
            {
                PS_LOG( "Scheduler::OnChangedWorkerState: sheduler doesn't know about worker"
                        " with ip = " << worker->GetIP() );
            }
        }

        if ( state == WORKER_STATE_FAILED )
        {
            PS_LOG( "TODO: Scheduler::OnChangedWorkerState" );
        }
    }
}

void Scheduler::OnNewJob( Job *job )
{
    if ( CanTakeNewJob() )
        PlanJobExecution();
}

void Scheduler::PlanJobExecution()
{
    Job *job = JobManager::Instance().PopJob();
    if ( !job )
        return;

    int totalCPU = WorkerManager::Instance().GetTotalCPU();
    int numExec = job->GetMaxCPU();
    if ( numExec < 0 || numExec > totalCPU )
        numExec = totalCPU;
    if ( numExec < 1 )
        numExec = 1;

    job->SetNumPlannedExec( numExec );

    int64_t jobId = job->GetJobId();
    {
        boost::mutex::scoped_lock scoped_lock( jobsMut_ );

        std::set< int > &tasks = tasksToSend_[ jobId ];
        for( int taskId = 0; taskId < numExec; ++taskId )
        {
            tasks.insert( taskId );
        }

        jobs_.Add( job, numExec );
    }

    NotifyAll();
}

bool Scheduler::RescheduleJob( const WorkerJob &workerJob )
{
    bool found = true;
    std::set<int64_t> jobs;
    workerJob.GetJobs( jobs );

    std::set<int64_t>::const_iterator it = jobs.begin();

    boost::mutex::scoped_lock scoped_lock( jobsMut_ );

    for( ; it != jobs.end(); ++it )
    {
        int64_t jobId = *it;
        const Job *job = jobs_.FindJobByJobId( jobId );
        if ( job )
        {
            size_t failedNodesCnt = failedWorkers_.GetFailedNodesCnt( jobId );
            if ( failedNodesCnt >= (size_t)job->GetMaxFailedNodes() )
            {
                StopWorkers( jobId );
                jobs_.RemoveJob( jobId, "max failed nodes limit exceeded" );
                continue;
            }

            if ( job->IsNoReschedule() )
            {
                jobs_.DecrementJobExecution( jobId, workerJob.GetNumTasks( jobId ) );
                continue;
            }

            WorkerJob::Tasks tasks;
            workerJob.GetTasks( jobId, tasks );
            WorkerJob::Tasks::const_iterator it_task = tasks.begin();
            for( ; it_task != tasks.end(); ++it_task )
            {
                int taskId = *it_task;
                needReschedule_.push_back( WorkerTask( jobId, taskId ) );
                found = true;
            }
        }
        else
        {
            PS_LOG( "Scheduler::RescheduleJob: Job for jobId=" << jobId << " not found" );
        }
    }
    return found;
}

bool Scheduler::GetJobForWorker( const Worker *worker, WorkerJob &workerJob, int numCPU )
{
    int64_t jobId;
    bool foundReschedJob = false;

    // firstly, check if there is a task which needs to reschedule
    if ( !needReschedule_.empty() )
    {
        TaskList::iterator it = needReschedule_.begin();
        for( ; it != needReschedule_.end(); )
        {
            if ( workerJob.GetTotalNumTasks() >= numCPU )
                break;

            const WorkerTask &workerTask = *it;

            if ( foundReschedJob )
            {
                if ( workerTask.GetJobId() == jobId )
                {
                    workerJob.AddTask( jobId, workerTask.GetTaskId() );
                    needReschedule_.erase( it++ );
                    continue;
                }
            }
            else
            {
                jobId = workerTask.GetJobId();
                if ( !failedWorkers_.IsWorkerFailedJob( worker->GetIP(), jobId ) )
                {
                    foundReschedJob = true;
                    workerJob.AddTask( jobId, workerTask.GetTaskId() );
                    needReschedule_.erase( it++ );
                    continue;
                }
            }
            ++it;
        }
    }

    const ScheduledJobs::JobList &jobs = jobs_.GetJobList();
    ScheduledJobs::JobList::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        const Job *j = *it;

        if ( foundReschedJob && jobId != j->GetJobId() )
            continue;

        if ( failedWorkers_.IsWorkerFailedJob( worker->GetIP(), j->GetJobId() ) )
            continue;

        JobIdToTasks::iterator it = tasksToSend_.find( j->GetJobId() );
        if ( it == tasksToSend_.end() )
            continue;

        std::set< int > &tasks = it->second;
        if ( !tasks.empty() )
        {
            std::set< int >::iterator it_task = tasks.begin();
            for( ; it_task != tasks.end();  )
            {
                if ( workerJob.GetTotalNumTasks() >= numCPU )
                    break;
                int taskId = *it_task;
                workerJob.AddTask( j->GetJobId(), taskId );

                tasks.erase( it_task++ );
                if ( tasks.empty() )
                {
                    tasksToSend_.erase( it );
                    break;
                }
            }
            break;
        }
    }

    return workerJob.GetTotalNumTasks() > 0;
}

bool Scheduler::GetTaskToSend( WorkerJob &workerJob, std::string &hostIP, Job **job )
{
    boost::mutex::scoped_lock scoped_lock_w( workersMut_ );

    workerPriority_.Sort( nodeState_.begin(), nodeState_.end(), nodeState_.size()/*, CompareByCPU()*/ );

    boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );

    WorkerPriority::iterator it = workerPriority_.Begin();
    for( ; it != workerPriority_.End(); ++it )
    {
        NodeState &nodeState = *(*it);
        int freeCPU = nodeState.GetNumFreeCPU();
        if ( freeCPU <= 0 )
            continue;

        Worker *w = nodeState.GetWorker();

        if ( GetJobForWorker( w, workerJob, freeCPU ) )
        {
            *job = jobs_.FindJobByJobId( workerJob.GetJobId() );
            if ( !*job )
            {
                PS_LOG( "Scheduler::GetTaskToSend: job not found for jobId=" << workerJob.GetJobId() );
                continue;
            }
            w->GetJob() += workerJob;
            hostIP = w->GetIP();

            nodeState.AllocCPU( workerJob.GetTotalNumTasks() );
            return true;
        }
    }

    // if there is any worker available, but all queued jobs are
    // sended to workers, then take next job from job mgr queue
    scoped_lock_j.unlock();
    scoped_lock_w.unlock();
    PlanJobExecution();

    return false;
}

void Scheduler::OnTaskSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const Job *job )
{
    if ( !success )
    {
        {
            Worker *w = WorkerManager::Instance().GetWorkerByIP( hostIP );

            PS_LOG( "Scheduler::OnTaskSendCompletion: job sending failed."
                    " jobId=" << workerJob.GetJobId() << ", ip=" << hostIP );

            boost::mutex::scoped_lock scoped_lock( workersMut_ );
            {
                boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );
                if ( !jobs_.FindJobByJobId( workerJob.GetJobId() ) )
                    return;
            }

            failedWorkers_.Add( workerJob.GetJobId(), hostIP );

            // job need to be rescheduled to any other node
            RescheduleJob( w->GetJob() );

            NodeState &nodeState = nodeState_[ hostIP ];
            nodeState.FreeCPU( workerJob.GetTotalNumTasks() );
            w->ResetJob();
        }
        NotifyAll();
    }
}

void Scheduler::OnTaskCompletion( int errCode, const WorkerTask &workerTask, const std::string &hostIP )
{
    PS_LOG( "Scheduler::OnTaskCompletion " << errCode );

    if ( !errCode )
    {
        Worker *w = WorkerManager::Instance().GetWorkerByIP( hostIP );
        boost::mutex::scoped_lock scoped_lock_w( workersMut_ );
        boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );

        if ( !jobs_.FindJobByJobId( workerTask.GetJobId() ) )
            return;

        WorkerJob &workerJob = w->GetJob();
        if ( !workerJob.DeleteTask( workerTask.GetJobId(), workerTask.GetTaskId() ) )
        {
            // task already processed
            // it can be when a few threads simultaneously gets success errCode from the same task
            // or after timeout
            return;
        }

        NodeState &nodeState = nodeState_[ hostIP ];
        nodeState.FreeCPU( 1 );

        jobs_.DecrementJobExecution( workerTask.GetJobId(), 1 );
    }
    else
    {
        if ( errCode == NODE_JOB_COMPLETION_NOT_FOUND )
            return;

        Worker *w = WorkerManager::Instance().GetWorkerByIP( hostIP );
        boost::mutex::scoped_lock scoped_lock( workersMut_ );
        {
            boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );
            if ( !jobs_.FindJobByJobId( workerTask.GetJobId() ) )
                return;
        }

        PS_LOG( "Scheduler::OnTaskCompletion: errCode=" << errCode <<
                ", jobId=" << workerTask.GetJobId() << ", ip=" << hostIP );

        failedWorkers_.Add( workerTask.GetJobId(), hostIP );

        const WorkerJob &workerJob = w->GetJob();
        // job need to be rescheduled to any other node
        RescheduleJob( workerJob );

        NodeState &nodeState = nodeState_[ hostIP ];
        nodeState.FreeCPU( workerJob.GetTotalNumTasks() );
        w->ResetJob();
    }

    NotifyAll();
}

void Scheduler::OnTaskTimeout( const WorkerTask &workerTask, const std::string &hostIP )
{
    const Worker *w = WorkerManager::Instance().GetWorkerByIP( hostIP );
    const WorkerJob &workerJob = w->GetJob();

    if ( workerJob.HasTask( workerTask.GetJobId(), workerTask.GetTaskId() ) )
    {
        PS_LOG( "Scheduler::OnTaskTimeout " << workerTask.GetJobId() << ":" << workerTask.GetTaskId() << " " << hostIP );

        // send stop command to worker
        StopTaskCommand *stopCommand = new StopTaskCommand();
        stopCommand->SetParam( "job_id", workerTask.GetJobId() );
        stopCommand->SetParam( "task_id", workerTask.GetTaskId() );
        CommandPtr commandPtr( stopCommand );
        WorkerManager::Instance().AddCommand( commandPtr, hostIP );

        OnTaskCompletion( NODE_JOB_TIMEOUT, workerTask, hostIP );
    }
}

void Scheduler::OnJobTimeout( int64_t jobId )
{
    {
        boost::mutex::scoped_lock scoped_lock_w( workersMut_ );
        boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );

        if ( !jobs_.FindJobByJobId( jobId ) )
            return;
        StopWorkers( jobId );
        jobs_.RemoveJob( jobId, "timeout" );
    }
    NotifyAll();
}

void Scheduler::OnRemoveJob( int64_t jobId )
{
    failedWorkers_.Delete( jobId );
}

void Scheduler::StopWorkers( int64_t jobId )
{
    {
        IPToNodeState::iterator it = nodeState_.begin();
        for( ; it != nodeState_.end(); ++it )
        {
            NodeState &nodeState = it->second;
            Worker *worker = nodeState.GetWorker();
            WorkerJob &workerJob = worker->GetJob();

            if ( workerJob.HasJob( jobId ) )
            {
                // send stop command to worker
                WorkerJob::Tasks tasks;
                workerJob.GetTasks( jobId, tasks );
                WorkerJob::Tasks::const_iterator it_task = tasks.begin();
                for( ; it_task != tasks.end(); ++it_task )
                {
                    StopTaskCommand *stopCommand = new StopTaskCommand();
                    stopCommand->SetParam( "job_id", jobId );
                    stopCommand->SetParam( "task_id", *it_task );
                    CommandPtr commandPtr( stopCommand );
                    WorkerManager::Instance().AddCommand( commandPtr, worker->GetIP() );
                }

                nodeState.FreeCPU( workerJob.GetNumTasks( jobId ) );
                workerJob.DeleteJob( jobId );
            }
        }
    }

    tasksToSend_.erase( jobId );
    {
        TaskList::iterator it = needReschedule_.begin();
        for( ; it != needReschedule_.end(); )
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

bool Scheduler::CanTakeNewJob() const
{
    IPToNodeState::const_iterator it = nodeState_.begin();
    for( ; it != nodeState_.end(); ++it )
    {
        const NodeState &nodeState = it->second;
        if ( nodeState.GetNumFreeCPU() > 0 )
            return true;
    }

    return false;
}

void Scheduler::Shutdown()
{
    jobs_.Clear();
}

void Scheduler::GetJobInfo( std::string &info, int64_t jobId )
{
    std::ostringstream ss;
    boost::mutex::scoped_lock scoped_lock_w( workersMut_ );
    boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );

    Job *job = jobs_.FindJobByJobId( jobId );
    if ( !job )
    {
        ss << "job isn't executing now, jobId = " << jobId;
        info = ss.str();
        return;
    }

    ss << "================" << std::endl <<
        "Job info, jobId = " << job->GetJobId() << std::endl;

    {
        int totalExec = job->GetNumPlannedExec();
        int numExec = totalExec - jobs_.GetNumExec( jobId );
        ss << "job executions = " << numExec << std::endl <<
            "total planned executions = " << totalExec << std::endl;
    }

    {
        int num = 0;
        IPToNodeState::const_iterator it = nodeState_.begin();
        for( ; it != nodeState_.end(); ++it )
        {
            const NodeState &nodeState = it->second;
            const WorkerJob &workerJob = nodeState.GetWorker()->GetJob();

            if ( workerJob.HasJob( jobId ) )
                ++num;
        }
        ss << "busy workers = " << num << std::endl;
    }

    {
        int num = 0;
        IPToNodeState::const_iterator it = nodeState_.begin();
        for( ; it != nodeState_.end(); ++it )
        {
            const NodeState &nodeState = it->second;
            const WorkerJob &workerJob = nodeState.GetWorker()->GetJob();

            if ( workerJob.HasJob( jobId ) )
                num += nodeState.GetNumBusyCPU();
        }
        ss << "busy cpu's = " << num << std::endl;
    }

    ss << "================";
    info = ss.str();
}

void Scheduler::GetStatistics( std::string &stat )
{
    std::ostringstream ss;

    boost::mutex::scoped_lock scoped_lock_w( workersMut_ );
    boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );

    ss << "================" << std::endl <<
        "busy workers = " << GetNumBusyWorkers() << std::endl <<
        "free workers = " << GetNumFreeWorkers() << std::endl <<
        "failed jobs = " << failedWorkers_.GetFailedJobsCnt() << std::endl <<
        "busy cpu's = " << GetNumBusyCPU() << std::endl <<
        "total cpu's = " << WorkerManager::Instance().GetTotalCPU() << std::endl;

    ss << "jobs = " << jobs_.GetNumJobs() << std::endl <<
        "need reschedule = " << needReschedule_.size() << std::endl;

    ss << "executing jobs: {";
    const ScheduledJobs::JobList &jobs = jobs_.GetJobList();
    ScheduledJobs::JobList::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        if ( it != jobs.begin() )
            ss << ", ";
        const Job *job = *it;
        ss << job->GetJobId();
    }
    ss << "}" << std::endl;

    ss << "================";

    stat = ss.str();
}

int Scheduler::GetNumBusyWorkers() const
{
    int num = 0;
    IPToNodeState::const_iterator it = nodeState_.begin();
    for( ; it != nodeState_.end(); ++it )
    {
        const NodeState &nodeState = it->second;
        if ( nodeState.GetNumBusyCPU() > 0 )
            ++num;
    }
    return num;
}

int Scheduler::GetNumFreeWorkers() const
{
    int num = 0;
    IPToNodeState::const_iterator it = nodeState_.begin();
    for( ; it != nodeState_.end(); ++it )
    {
        const NodeState &nodeState = it->second;
        if ( nodeState.GetNumBusyCPU() <= 0 )
            ++num;
    }
    return num;
}

int Scheduler::GetNumBusyCPU() const
{
    int num = 0;
    IPToNodeState::const_iterator it = nodeState_.begin();
    for( ; it != nodeState_.end(); ++it )
    {
        const NodeState &nodeState = it->second;
        num += nodeState.GetNumBusyCPU();
    }
    return num;
}

} // namespace master

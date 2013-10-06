#include "scheduler.h"
#include "common/log.h"
#include "common/error_code.h"
#include "job_manager.h"
#include "worker_manager.h"

// hint: avoid deadlocks. always lock jobs mutex after workers mutex

namespace master {

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
                const WorkerJob workerJob = worker->GetJob();
                int64_t jobId = workerJob.GetJobId();

                PS_LOG( "Scheduler::OnChangedWorkerState: worker isn't available, while executing job"
                        "; nodeIP=" << worker->GetIP() << ", jobId=" << jobId );

                failedWorkers_.Add( jobId, worker->GetIP() );
                nodeState.Reset();
                worker->ResetJob();

                if ( RescheduleTask( workerJob ) )
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
    int numExec = static_cast<int>( totalCPU * ( job->GetMaxCPU() / 100. ) );
    if ( numExec < 1 )
        numExec = 1;
    if ( numExec > totalCPU )
        numExec = totalCPU;

    job->SetNumPlannedExec( numExec );

    int64_t jobId = job->GetJobId();
    {
        boost::mutex::scoped_lock scoped_lock( jobsMut_ );

        std::set< int > &tasks = tasksToSend_[ jobId ];
        for( int taskId = 0; taskId < numExec; ++taskId )
        {
            tasks.insert( taskId );
        }

        jobExecutions_[ jobId ] = numExec;
        jobs_.push_back( job );
    }

    NotifyAll();
}

bool Scheduler::RescheduleTask( const WorkerJob &workerJob )
{
    int64_t jobId = workerJob.GetJobId();
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    const Job *job = FindJobByJobId( jobId );
    if ( job )
    {
        size_t failedNodesCnt = failedWorkers_.GetFailedNodesCnt( jobId );
        if ( failedNodesCnt >= (size_t)job->GetMaxFailedNodes() )
        {
            scoped_lock.unlock();
            StopWorkers( jobId );
            RemoveJob( jobId, "max failed nodes limit exceeded" );
            return false;
        }

        if ( job->IsNoReschedule() )
        {
            DecrementJobExecution( jobId, workerJob.GetNumTasks() );
            return false;
        }

        const WorkerJob::Tasks &tasks = workerJob.GetTasks();
        WorkerJob::Tasks::const_iterator it = tasks.begin();
        for( ; it != tasks.end(); ++it )
        {
            int taskId = *it;
            needReschedule_.push_back( WorkerTask( jobId, taskId ) );
        }
        return true;
    }
    else
    {
        PS_LOG( "Scheduler::RescheduleTask: Job for jobId=" << jobId << " not found" );
    }
    return false;
}

bool Scheduler::GetJobForWorker( const Worker *worker, WorkerJob &workerJob, int numCPU )
{
    int64_t jobId;
    bool foundReschedJob = false;

    // firstly, check if there is a task which needs to reschedule
    if ( !needReschedule_.empty() )
    {
        std::list< WorkerTask >::iterator it = needReschedule_.begin();
        for( ; it != needReschedule_.end(); )
        {
            if ( workerJob.GetNumTasks() >= numCPU )
                break;

            const WorkerTask &workerTask = *it;

            if ( foundReschedJob )
            {
                if ( workerTask.GetJobId() == jobId )
                {
                    workerJob.AddTask( workerTask.GetTaskId() );
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
                    workerJob.AddTask( workerTask.GetTaskId() );
                    needReschedule_.erase( it++ );
                    continue;
                }
            }
            ++it;
        }
    }

    std::list< Job * >::const_iterator it = jobs_.begin();
    for( ; it != jobs_.end(); ++it )
    {
        const Job *j = *it;

        if ( foundReschedJob && jobId != j->GetJobId() )
            continue;

        if ( failedWorkers_.IsWorkerFailedJob( worker->GetIP(), j->GetJobId() ) )
            continue;

        std::set< int > &tasks = tasksToSend_[ j->GetJobId() ];
        if ( !tasks.empty() )
        {
            std::set< int >::iterator it_task = tasks.begin();
            for( ; it_task != tasks.end();  )
            {
                if ( workerJob.GetNumTasks() >= numCPU )
                    break;
                int taskId = *it_task;
                workerJob.AddTask( taskId );

                tasks.erase( it_task++ );
                if ( tasks.empty() )
                {
                    tasksToSend_.erase( jobId );
                    break;
                }
            }
            break;
        }
    }

    return workerJob.GetNumTasks() > 0;
}

bool Scheduler::GetTaskToSend( WorkerJob &workerJob, std::string &hostIP, Job **job )
{
    if ( freeWorkers_.empty() )
        return false;

    boost::mutex::scoped_lock scoped_lock_w( workersMut_ );
    if ( freeWorkers_.empty() )
        return false;

    boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );

    IPToNodeState::iterator it = nodeState_.begin();
    for( ; it != nodeState_.end(); ++it )
    {
        NodeState &nodeState = it->second;
        int freeCPU = nodeState.GetNumFreeCPU();
        if ( freeCPU <= 0 )
            continue;

        Worker *w = nodeState.GetWorker();

        if ( GetJobForWorker( w, workerJob, freeCPU ) )
        {
            w->SetJob( workerJob );
            hostIP = w->GetIP();
            *job = FindJobByJobId( workerJob.GetJobId() );

            int numBusyCPU = nodeState.GetNumBusyCPU() + workerJob.GetNumTasks();
            nodeState.SetNumBusyCPU( numBusyCPU );
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

            failedWorkers_.Add( workerJob.GetJobId(), hostIP );

            // job need to be rescheduled to any other node
            RescheduleTask( workerJob );

            NodeState &nodeState = nodeState_[ hostIP ];
            int numTasks = workerJob.GetNumTasks();
            nodeState.SetNumBusyCPU( nodeState.GetNumBusyCPU() - numTasks );
            w->ResetJob();
        }
        NotifyAll();
    }
}

void Scheduler::OnTaskCompletion( int errCode, const WorkerJob &workerJob, const std::string &hostIP )
{
    PS_LOG( "Scheduler::OnTaskCompletion " << errCode );
    if ( !errCode )
    {
        Worker *w = WorkerManager::Instance().GetWorkerByIP( hostIP );
        boost::mutex::scoped_lock scoped_lock_w( workersMut_ );

        IPToWorker::iterator it = busyWorkers_.find( hostIP );
        if ( it == busyWorkers_.end() ) // task already processed
            return;                     // it can be when a few threads simultaneously gets success errCode from the same task
                                        // or after timeout

        busyWorkers_.erase( it );
        freeWorkers_[ hostIP ] = w;
        w->SetJob( WorkerJob() );

        boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );
        DecrementJobExecution( workerJob.GetJobId() );
    }
    else
    {
        if ( errCode == NODE_JOB_COMPLETION_NOT_FOUND )
            return;

        Worker *w = WorkerManager::Instance().GetWorkerByIP( hostIP );
        boost::mutex::scoped_lock scoped_lock( workersMut_ );

        PS_LOG( "Scheduler::OnTaskCompletion: errCode=" << errCode <<
                ", jobId=" << workerJob.GetJobId() << ", ip=" << hostIP );

        failedWorkers_.Add( workerJob.GetJobId(), hostIP );

        // worker is free for now, to get another job
        busyWorkers_.erase( hostIP );
        freeWorkers_[ hostIP ] = w;
        // reset worker job
        w->SetJob( WorkerJob() );

        // job need to be rescheduled to any other node
        RescheduleTask( workerJob );
    }

    NotifyAll();
}

void Scheduler::OnTaskTimeout( const WorkerJob &workerJob, const std::string &hostIP )
{
    IPToWorker::iterator it;
    {
        boost::mutex::scoped_lock scoped_lock( workersMut_ );
        it = busyWorkers_.find( hostIP );
    }
    if ( it != busyWorkers_.end() )
    {
        const WorkerJob &job = it->second->GetJob();
        if ( job == workerJob )
        {
            PS_LOG( "Scheduler::OnJobTimeout " << workerJob.GetJobId() << ":" << workerJob.GetTaskId() << " " << hostIP );
            OnTaskCompletion( NODE_JOB_TIMEOUT, workerJob, hostIP );
        }
    }
}

void Scheduler::OnJobTimeout( int64_t jobId )
{
    {
        boost::mutex::scoped_lock scoped_lock( workersMut_ );

        if ( !FindJobByJobId( jobId ) )
            return;
        StopWorkers( jobId );
        RemoveJob( jobId, "timeout" );
    }
    NotifyAll();
}

void Scheduler::RunJobCallback( Job *job, const char *completionStatus )
{
    std::ostringstream ss;
    ss << "================" << std::endl <<
        "Job completed, jobId = " << job->GetJobId() << std::endl <<
        "completion status: " << completionStatus << std::endl <<
        "================";

    PS_LOG( ss.str() );

    job->RunCallback( ss.str() );
}

void Scheduler::DecrementJobExecution( int64_t jobId, int numTasks )
{
    int numExecution = jobExecutions_[ jobId ] - numTasks;
    jobExecutions_[ jobId ] = numExecution;
    if ( numExecution < 1 )
    {
        RemoveJob( jobId, "success" );
    }
}

void Scheduler::RemoveJob( int64_t jobId, const char *completionStatus )
{
    failedWorkers_.Delete( jobId );

    jobExecutions_.erase( jobId );
    std::list< Job * >::iterator it = jobs_.begin();
    for( ; it != jobs_.end(); ++it )
    {
        Job *job = *it;
        if ( job->GetJobId() == jobId )
        {
            RunJobCallback( job, completionStatus );
            jobs_.erase( it );
            delete job;
            return;
        }
    }

    PS_LOG( "Scheduler::RemoveJob: job not found for jobId=" << jobId );
}

void Scheduler::StopWorkers( int64_t jobId )
{
    {
        IPToWorker::iterator it = busyWorkers_.begin();
        for( ; it != busyWorkers_.end(); )
        {
            Worker *w = it->second;
            if ( w->GetJob().GetJobId() == jobId )
            {
                busyWorkers_.erase( it++ );
                freeWorkers_[ w->GetIP() ] = w;
                w->SetJob( WorkerJob() );
                continue;
            }
            ++it;
        }
    }
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    jobExecutions_.erase( jobId );
    tasksToSend_.erase( jobId );
    {
        std::list< WorkerJob >::iterator it = needReschedule_.begin();
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
    return !freeWorkers_.empty();
}

Job *Scheduler::FindJobByJobId( int64_t jobId ) const
{
    std::list< Job * >::const_iterator it = jobs_.begin();
    for( ; it != jobs_.end(); ++it )
    {
        Job *job = *it;
        if ( job->GetJobId() == jobId )
            return job;
    }
    return NULL;
}

void Scheduler::Shutdown()
{
    while( !jobs_.empty() )
    {
        delete jobs_.front();
        jobs_.pop_front();
    }
}

void Scheduler::GetJobInfo( std::string &info, int64_t jobId )
{
    std::ostringstream ss;
    boost::mutex::scoped_lock scoped_lock_w( workersMut_ );
    boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );

    Job *job = FindJobByJobId( jobId );
    if ( !job )
    {
        ss << "job isn't executing now, jobId = " << jobId;
        info = ss.str();
        return;
    }

    ss << "================" << std::endl <<
        "Job info, jobId = " << job->GetJobId() << std::endl;

    {
        std::map< int64_t, int >::const_iterator it = jobExecutions_.find( jobId );
        if ( it != jobExecutions_.end() )
        {
            int numNodes = job->GetNumPlannedExec();
            ss << "job executions = " << numNodes - it->second << std::endl <<
                "total planned executions = " << numNodes << std::endl;
        }
    }

    {
        int num = 0;
        IPToWorker::const_iterator it = busyWorkers_.begin();
        for( ; it != busyWorkers_.end(); ++it )
        {
            const WorkerJob &job = it->second->GetJob();
            if ( job.GetJobId() == jobId )
                ++num;
        }
        ss << "busy workers = " << num << std::endl;
    }

    {
        int num = 0;
        IPToWorker::const_iterator it = sendingJobWorkers_.begin();
        for( ; it != sendingJobWorkers_.end(); ++it )
        {
            const WorkerJob &job = it->second->GetJob();
            if ( job.GetJobId() == jobId )
                ++num;
        }
        ss << "sending workers = " << num << std::endl;
    }

    ss << "================";
    info = ss.str();
}

void Scheduler::GetStatistics( std::string &stat )
{
    std::ostringstream ss;
    ss << "================" << std::endl <<
        "busy workers = " << busyWorkers_.size() << std::endl <<
        "free workers = " << freeWorkers_.size() << std::endl <<
        "failed jobs = " << failedWorkers_.GetFailedJobsCnt() << std::endl <<
        "sending workers = " << sendingJobWorkers_.size() << std::endl;

    {
        boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );
        ss << "jobs = " << jobs_.size() << std::endl <<
            "need reschedule = " << needReschedule_.size() << std::endl;

        ss << "executing jobs: {";
        std::list< Job * >::const_iterator it = jobs_.begin();
        for( ; it != jobs_.end(); ++it )
        {
            if ( it != jobs_.begin() )
                ss << ", ";
            const Job *job = *it;
            ss << job->GetJobId();
        }
        ss << "}" << std::endl;
    }

    ss << "================";

    stat = ss.str();
}

} // namespace master

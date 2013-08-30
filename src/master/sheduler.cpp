#include "sheduler.h"
#include "common/log.h"
#include "common/error_code.h"
#include "job_manager.h"
#include "worker_manager.h"

// hint: avoid deadlocks. always lock jobs mutex after workers mutex

namespace master {

void Sheduler::OnHostAppearance( Worker *worker )
{
	{
		boost::mutex::scoped_lock scoped_lock( workersMut_ );
		freeWorkers_[ worker->GetIP() ] = worker;
	}
    NotifyAll();
}

void Sheduler::OnChangedWorkerState( const std::vector< Worker * > &workers )
{
    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    std::vector< Worker * >::const_iterator it = workers.begin();
    for( ; it != workers.end(); ++it )
    {
        const Worker *worker = *it;
        WorkerState state = worker->GetState();

        if ( state == WORKER_STATE_NOT_AVAIL )
        {
            IPToWorker::iterator it = busyWorkers_.find( worker->GetIP() );
            if ( it != busyWorkers_.end() )
            {
                const Worker *busyWorker = it->second;
                const WorkerJob &workerJob = busyWorker->GetJob();
                int64_t jobId = workerJob.jobId_;

                PS_LOG( "Sheduler::OnChangedWorkerState: worker isn't available, while executing job"
                        "; nodeIP=" << worker->GetIP() << ", jobId=" << jobId );

                failedWorkers_[ jobId ].insert( worker->GetIP() );
				busyWorkers_.erase( worker->GetIP() );

                const Job *job = JobManager::Instance().GetJobById( jobId );
                if ( job )
                {
                    size_t failedNodesCnt = failedWorkers_[ jobId ].size();
                    if ( failedNodesCnt < (size_t)job->GetMaxFailedNodes() )
                    {
                        boost::mutex::scoped_lock scoped_lock( jobsMut_ );
                        needReschedule_.push_back( workerJob );
                    }
                    else
                    {
                        PS_LOG( "Sheduler::OnChangedWorkerState: max failed nodes limit exceeded for job, jobId=" << jobId );
                    }
                }
                else
                {
                    PS_LOG( "Sheduler::OnChangedWorkerState: Job for jobId=" << jobId << " not found" );
                }
            }
			else
			{
				freeWorkers_.erase( worker->GetIP() );
			}
        }

        if ( state == WORKER_STATE_FAILED )
        {
            PS_LOG( "TODO: Sheduler::OnChangedWorkerState" );
        }
    }
}

void Sheduler::OnNewJob( Job *job )
{
    if ( CanTakeNewJob() )
		PlanJobExecution();
}

void Sheduler::PlanJobExecution()
{
	Job *job = JobManager::Instance().PopJob();
	if ( !job )
		return;

    int numNodes = job->GetNumNodes();
    if ( numNodes <= 0 )
    {
        // if there is no limit for max number of nodes,
        // set it to number of current active workers
        numNodes = WorkerManager::Instance().GetTotalWorkers();
        job->SetNumPlannedExec( numNodes );
    }

    int64_t jobId = job->GetJobId();
    {
        boost::mutex::scoped_lock scoped_lock( jobsMut_ );

        std::set< int > &tasks = tasksToSend_[ jobId ];
        for( int i = 0; i < numNodes; ++i )
        {
            tasks.insert( i );
        }

        jobExecutions_[ jobId ] = numNodes;
        jobs_.push_back( job );
    }

	NotifyAll();
}

bool Sheduler::SheduleTask( WorkerJob &workerJob, std::string &hostIP, Job **job,
                            int64_t jobId, int taskId, bool reschedule )
{
    IPToWorker::const_iterator it = freeWorkers_.begin();
    for( ; it != freeWorkers_.end(); ++it )
    {
        Worker *w = it->second;
        if ( !CheckIfWorkerFailedJob( w, jobId ) )
        {
            if ( !reschedule )
            {
                std::set< int > &tasks = tasksToSend_[ jobId ];
                tasks.erase( taskId );
                if ( tasks.empty() )
                    tasksToSend_.erase( jobId );
            }
            else
            {
                needReschedule_.pop_front();
            }

			freeWorkers_.erase( w->GetIP() );
			sendingJobWorkers_[ w->GetIP() ] = w;

			workerJob = WorkerJob( jobId, taskId );
            w->SetJob( workerJob );

			hostIP = w->GetIP();
            *job = jobs_.front();
            return true;
        }
    }
    return false;
}

bool Sheduler::GetTaskToSend( WorkerJob &workerJob, std::string &hostIP, Job **job )
{
    if ( freeWorkers_.empty() )
        return false;

    boost::mutex::scoped_lock scoped_lock_w( workersMut_ );
    if ( freeWorkers_.empty() )
        return false;

    boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );

    int64_t jobId;
    int taskId;

    // firstly, check if there is a task which needs to reschedule
    if ( !needReschedule_.empty() )
    {
        std::list< WorkerJob >::const_iterator it = needReschedule_.begin();
        for( ; it != needReschedule_.end(); ++it )
        {
            workerJob = *it;
            jobId = workerJob.jobId_;
            taskId = workerJob.taskId_;

            if ( SheduleTask( workerJob, hostIP, job,
                              jobId, taskId, true ) )
                return true;
        }
    }

    std::list< Job * >::const_iterator it = jobs_.begin();
    for( ; it != jobs_.end(); ++it )
    {
        const Job *j = *it;
        jobId = j->GetJobId();

        const std::set< int > &tasks = tasksToSend_[ jobId ];
        if ( !tasks.empty() )
        {
            taskId = *tasks.begin();
            if ( SheduleTask( workerJob, hostIP, job,
                              jobId, taskId, false ) )
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

void Sheduler::OnTaskSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const Job *job )
{
    if ( success )
    {
        Worker *w = WorkerManager::Instance().GetWorkerByIP( hostIP );
        boost::mutex::scoped_lock scoped_lock( workersMut_ );
        sendingJobWorkers_.erase( hostIP );
        busyWorkers_[ hostIP ] = w;
    }
    else
    {
        {
            Worker *w = WorkerManager::Instance().GetWorkerByIP( hostIP );
            boost::mutex::scoped_lock scoped_lock_w( workersMut_ );

            PS_LOG( "Sheduler::OnTaskSendCompletion: job sending failed."
                    " jobId=" << workerJob.jobId_ << ", ip=" << hostIP );

            failedWorkers_[ workerJob.jobId_ ].insert( hostIP );

            // worker is free for now, to get another job
            sendingJobWorkers_.erase( hostIP );
            freeWorkers_[ hostIP ] = w;
            // reset worker job
            w->SetJob( WorkerJob() );

            // job need to be rescheduled to any other node
            boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );
            needReschedule_.push_back( workerJob );
        }
        NotifyAll();
    }
}

void Sheduler::OnTaskCompletion( int errCode, const WorkerJob &workerJob, const std::string &hostIP )
{
    PS_LOG( "Sheduler::OnTaskCompletion " << errCode );
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
        int numExecution = jobExecutions_[ workerJob.jobId_ ];
        if ( numExecution <= 1 )
        {
            RemoveJob( workerJob.jobId_ );
        }
        else
        {
            jobExecutions_[ workerJob.jobId_ ] = numExecution - 1;
        }
    }
    else
    {
        if ( errCode == NODE_JOB_COMPLETION_NOT_FOUND )
            return;

        Worker *w = WorkerManager::Instance().GetWorkerByIP( hostIP );
        boost::mutex::scoped_lock scoped_lock_w( workersMut_ );

        PS_LOG( "Sheduler::OnTaskCompletion: errCode=" << errCode <<
                ", jobId=" << workerJob.jobId_ << ", ip=" << hostIP );

        failedWorkers_[ workerJob.jobId_ ].insert( hostIP );

        // worker is free for now, to get another job
        busyWorkers_.erase( hostIP );
        freeWorkers_[ hostIP ] = w;
        // reset worker job
        w->SetJob( WorkerJob() );

        // job need to be rescheduled to any other node
        boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );
        needReschedule_.push_back( workerJob );
    }

    NotifyAll();
}

void Sheduler::OnJobTimeout( const WorkerJob &workerJob, const std::string &hostIP )
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
            PS_LOG( "Sheduler::OnJobTimeout " << workerJob.jobId_ << ":" << workerJob.taskId_ << " " << hostIP );
            OnTaskCompletion( NODE_JOB_TIMEOUT, workerJob, hostIP );
        }
    }
}

void Sheduler::RunJobCallback( Job *job )
{
    std::ostringstream ss;
    ss << "================" << std::endl <<
        "Job completed, jobId = " << job->GetJobId() << std::endl <<
        "================";

    job->RunCallback( ss.str() );
}

void Sheduler::RemoveJob( int64_t jobId )
{
    std::map< int64_t, std::set< std::string > >::iterator it_failed(
        failedWorkers_.find( jobId )
    );
    if ( it_failed != failedWorkers_.end() )
    {
        int numFailed = it_failed->second.size();
        failedWorkers_.erase( it_failed );
        PS_LOG( "Sheduler::RemoveJob: jobId=" << jobId << ", num failed workers=" << numFailed );
    }

    jobExecutions_.erase( jobId );
    std::list< Job * >::iterator it = jobs_.begin();
    for( ; it != jobs_.end(); ++it )
    {
        Job *job = *it;
        if ( job->GetJobId() == jobId )
        {
            RunJobCallback( job );
            jobs_.erase( it );
            delete job;
            return;
        }
    }

    PS_LOG( "Sheduler::RemoveJob: job not found for jobId=" << jobId );
}

bool Sheduler::CheckIfWorkerFailedJob( Worker *worker, int64_t jobId ) const
{
    const std::string &ip = worker->GetIP();
    std::map< int64_t, std::set< std::string > >::const_iterator it = failedWorkers_.find( jobId );
    if ( it == failedWorkers_.end() )
        return false;

    const std::set< std::string > &ips = it->second;
    std::set< std::string >::const_iterator it_ip = ips.begin();
    for( ; it_ip != ips.end(); ++it_ip )
    {
        if ( ip == *it_ip )
            return true;
    }
    return false;
}

bool Sheduler::CanTakeNewJob() const
{
    return !freeWorkers_.empty();
}

Job *Sheduler::FindJobByJobId( int64_t jobId ) const
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

/*bool Sheduler::IsWorkerBusy( const std::string &hostIP, int64_t jobId, int taskId )
{
    boost::mutex::scoped_lock scoped_lock( workersMut_ );
    IPToWorker::const_iterator it = busyWorkers_.find( hostIP );
    if ( it == busyWorkers_.end() )
        return false;
    const Worker *worker = it->second;
    const WorkerJob &workerJob = worker->GetJob();
    return ( workerJob.jobId_ == jobId ) && ( workerJob.taskId_ == taskId );
}*/

void Sheduler::Shutdown()
{
	while( !jobs_.empty() )
	{
		delete jobs_.front();
		jobs_.pop_front();
	}
}

void Sheduler::GetJobInfo( std::string &info, int64_t jobId )
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
            if ( job.jobId_ == jobId )
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
            if ( job.jobId_ == jobId )
                ++num;
        }
        ss << "sending workers = " << num << std::endl;
    }

    ss << "================";
    info = ss.str();
}

void Sheduler::GetStatistics( std::string &stat )
{
    std::ostringstream ss;
    ss << "================" << std::endl <<
        "busy workers = " << busyWorkers_.size() << std::endl <<
        "free workers = " << freeWorkers_.size() << std::endl <<
        "failed workers = " << failedWorkers_.size() << std::endl <<
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

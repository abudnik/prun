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
	PlanJobExecution();

    if ( NeedToSendTask() )
        NotifyAll();
}

void Sheduler::OnChangedWorkerState( const std::vector< Worker * > &workers )
{
    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    std::vector< Worker * >::const_iterator it = workers.begin();
    for( ; it != workers.end(); ++it )
    {
        Worker *worker = *it;
        WorkerState state = worker->GetState();

        if ( state == WORKER_STATE_NOT_AVAIL )
        {
            IPToWorker::iterator it = busyWorkers_.find( worker->GetIP() );
            if ( it != busyWorkers_.end() )
            {
                Worker *busyWorker = it->second;
                const WorkerJob &workerJob = busyWorker->GetJob();
                int64_t jobId = workerJob.jobId_;

                PS_LOG( "Sheduler::OnChangedWorkerState: worker isn't available, while executing job"
                        "; nodeIP=" << worker->GetIP() << ", jobId=" << jobId );

                failedWorkers_[ jobId ].insert( worker->GetIP() );
				busyWorkers_.erase( worker->GetIP() );

                Job *job = JobManager::Instance().GetJobById( jobId );
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

void Sheduler::PlanJobExecution()
{
	Job *job = JobManager::Instance().PopJob();
	if ( !job )
		return;

    int numNodes = job->GetNumNodes();
    if ( numNodes <= 0 )
        numNodes = WorkerManager::Instance().GetTotalWorkers();

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

void Sheduler::OnNewJob( Job *job )
{
    if ( CanTakeNewJob() )
		PlanJobExecution();
}

bool Sheduler::GetTaskToSend( WorkerJob &workerJob, std::string &hostIP, Job **job )
{
    if ( !NeedToSendTask() )
        return false;

    boost::mutex::scoped_lock scoped_lock_w( workersMut_ );
    if ( freeWorkers_.empty() )
        return false;

    boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );

    int64_t jobId;
    int taskId;
    bool needReschedule = false;

    // get task
    if ( !needReschedule_.empty() )
    {
        WorkerJob workerJob = needReschedule_.front();
        jobId = workerJob.jobId_;
        taskId = workerJob.taskId_;
        needReschedule = true;
    }
    else
    {
        if ( tasksToSend_.empty() )
            return false;

        Job *j = jobs_.front();
        jobId = j->GetJobId();

        std::set< int > &tasks = tasksToSend_[ jobId ];
        taskId = *tasks.begin();
    }

    // find available worker
    IPToWorker::const_iterator it = freeWorkers_.begin();
    for( ; it != freeWorkers_.end(); ++it )
    {
        Worker *w = it->second;
        if ( !CheckIfWorkerFailedJob( w, jobId ) )
        {
            if ( !needReschedule )
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
            jobs_.erase( it );
            delete job;
            break;
        }
    }

    PrintStats(); // dbg only?
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

bool Sheduler::NeedToSendTask() const
{
    return ( !freeWorkers_.empty() ) && ( !tasksToSend_.empty() || !needReschedule_.empty() );
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

void Sheduler::PrintStats()
{
    PS_LOG( "================" );
    PS_LOG( "free workers = " << freeWorkers_.size() );
    PS_LOG( "failed workers = " << failedWorkers_.size() );
    PS_LOG( "sending workers = " << sendingJobWorkers_.size() );

    PS_LOG( "jobs = " << jobs_.size() );
    PS_LOG( "need reschedule = " << needReschedule_.size() );
    PS_LOG( "================" );
}

void Sheduler::Shutdown()
{
	while( !jobs_.empty() )
	{
		delete jobs_.front();
		jobs_.pop_front();
	}
}

} // namespace master

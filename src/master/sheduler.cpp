#include "sheduler.h"
#include "common/log.h"
#include "job_manager.h"
#include "worker_manager.h"

// hint: avoid deadlocks. always lock jobs mutex after workers mutex

namespace master {

void Sheduler::OnHostAppearance( Worker *worker )
{
    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    freeWorkers_[ worker->GetIP() ] = worker;
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
            freeWorkers_.erase( worker->GetIP() );
        }

        if ( state == WORKER_STATE_FAILED )
        {
            IPToWorker::iterator it = busyWorkers_.find( worker->GetIP() );
            if ( it != busyWorkers_.end() )
            {
                Worker *busyWorker = it->second;
                const WorkerJob &workerJob = busyWorker->GetJob();
                int64_t jobId = workerJob.jobId_;

                failedWorkers_[ jobId ].insert( worker->GetIP() );

                Job *job = JobManager::Instance().GetJobById( jobId );
                if ( job )
                {
                    size_t failedNodesCnt = failedWorkers_[ jobId ].size();
                    if ( failedNodesCnt < (size_t)job->GetMaxFailedNodes() )
                    {
                        boost::mutex::scoped_lock scoped_lock( jobsMut_ );
                        needReschedule_.push( workerJob );
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
        }
    }
}

void Sheduler::OnNewJob( Job *job )
{
    if ( !CanTakeNewJob() )
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
        jobs_.push( job );
    }

    JobManager::Instance().PopJob();
}

bool Sheduler::GetTaskToSend( Worker **worker, Job **job )
{
    if ( !NeedToSendTask() )
        return false;

    boost::mutex::scoped_lock scoped_lock_w( workersMut_ );
    if ( !freeWorkers_.size() )
        return false;

    boost::mutex::scoped_lock scoped_lock_j( jobsMut_ );

    int64_t jobId;
    int taskId;

    // get task
    if ( needReschedule_.size() )
    {
    }
    else
    {
        if ( !tasksToSend_.size() )
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
            tasksToSend_[ jobId ].erase( taskId );
            WorkerJob workerJob( jobId, taskId );
            w->SetJob( workerJob );
            *worker = w;
            *job = jobs_.front();
            return true;
        }
    }

    return false;
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
    return freeWorkers_.size() > 0;
}

bool Sheduler::NeedToSendTask() const
{
    return ( freeWorkers_.size() > 0 ) && ( tasksToSend_.size() > 0 || needReschedule_.size() > 0 );
}

} // namespace master

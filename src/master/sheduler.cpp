#include "sheduler.h"
#include "job_manager.h"
#include "common/log.h"

namespace master {

void Sheduler::OnHostAppearance( Worker *worker )
{
    freeWorkers[ worker->GetIP() ] = worker;
}

void Sheduler::OnChangedWorkerState( const std::vector< Worker * > &workers )
{
    std::vector< Worker * >::const_iterator it = workers.begin();
    for( ; it != workers.end(); ++it )
    {
        Worker *worker = *it;
        WorkerState state = worker->GetState();

        if ( state == WORKER_STATE_NOT_AVAIL )
        {
            freeWorkers.erase( worker->GetIP() );
        }

        if ( state == WORKER_STATE_FAILED )
        {
            IPToWorker::iterator it = busyWorkers.find( worker->GetIP() );
            if ( it != busyWorkers.end() )
            {
                Worker *busyWorker = it->second;
                const WorkerJob &workerJob = busyWorker->GetJob();
                int64_t jobId = workerJob.jobId_;

                failedWorkers[ jobId ].insert( worker->GetIP() );
                
                Job *job = JobManager::Instance().GetJobById( jobId );
                if ( job )
                {
                    size_t failedNodesCnt = failedWorkers[ jobId ].size();
                    if ( failedNodesCnt < job->GetMaxFailedNodes() )
                    {
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

} // namespace master

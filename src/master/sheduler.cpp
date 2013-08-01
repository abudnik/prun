#include "sheduler.h"
#include "common/log.h"
#include "job_manager.h"
#include "worker_manager.h"

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
                    if ( failedNodesCnt < (size_t)job->GetMaxFailedNodes() )
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

void Sheduler::OnNewJob( Job *job )
{
    if ( !CanTakeNewJob() )
        return;

    int numNodes = job->GetNumNodes();
    if ( numNodes <= 0 )
        numNodes = WorkerManager::Instance().GetTotalWorkers();

    int64_t jobId = job->GetJobId();

    std::set< int > &tasks = tasksToSend_[ jobId ];
    for( int i = 0; i < numNodes; ++i )
    {
        tasks.insert( i );
    }

    jobs_[ job ] = numNodes;

    JobManager::Instance().PopJob();
}

bool Sheduler::CanTakeNewJob() const
{
    return freeWorkers.size() > 0;
}

} // namespace master

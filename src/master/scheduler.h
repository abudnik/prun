#ifndef __SHEDULER_H
#define __SHEDULER_H

#include <set>
#include <list>
#include <boost/thread/mutex.hpp>
#include "common/observer.h"
#include "worker.h"
#include "job.h"
#include "failed_workers.h"


namespace master {

class NodeState
{
public:
    NodeState()
    : numBusyCPU_( 0 ),
     worker_( NULL )
    {}

    void Reset()
    {
        numBusyCPU_ = 0;
    }

    int GetNumBusyCPU() const { return numBusyCPU_; }
    void SetNumBusyCPU( int num ) { numBusyCPU_ = num; }
    int GetNumFreeCPU() const { return worker_->GetNumCPU() - numBusyCPU_; }
    void SetWorker( Worker *w ) { worker_ = w; }
    Worker *GetWorker() const { return worker_; }

private:
    int numBusyCPU_;
    Worker *worker_;

};
typedef std::map< std::string, NodeState > IPToNodeState;


class Scheduler : public python_server::Observable< true >
{
public:
    void OnHostAppearance( Worker *worker );

    void OnChangedWorkerState( const std::vector< Worker * > &workers );

    void OnNewJob( Job *job );

    bool GetTaskToSend( WorkerJob &workerJob, std::string &hostIP, Job **job );

    void OnTaskSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const Job *job );

    void OnTaskCompletion( int errCode, const WorkerTask &workerTask, const std::string &hostIP );

    void OnTaskTimeout( const WorkerTask &workerTask, const std::string &hostIP );
    void OnJobTimeout( int64_t jobId );

    void GetJobInfo( std::string &info, int64_t jobId );
    void GetStatistics( std::string &stat );

    static Scheduler &Instance()
    {
        static Scheduler instance_;
        return instance_;
    }

    void Shutdown();

private:
    void PlanJobExecution();
    bool RescheduleJob( const WorkerJob &workerJob );
    bool GetJobForWorker( const Worker *worker, WorkerJob &workerJob, int numCPU );

    void RunJobCallback( Job *job, const char *completionStatus );
    void DecrementJobExecution( int64_t jobId, int numTasks );
    void RemoveJob( int64_t jobId, const char *completionStatus );
    void StopWorkers( int64_t jobId );

    bool CanTakeNewJob() const;

    Job *FindJobByJobId( int64_t jobId ) const;

    // stats
    int GetNumBusyWorkers() const;
    int GetNumFreeWorkers() const;
    int GetNumBusyCPU() const;

private:
    IPToNodeState nodeState_;
    FailedWorkers failedWorkers_;
    boost::mutex workersMut_;

    std::list< Job * > jobs_;
    std::map< int64_t, int > jobExecutions_; // job_id -> num job remaining executions (== 0, if job execution completed)
    std::map< int64_t, std::set< int > > tasksToSend_; // job_id -> set(task_id)
    std::list< WorkerTask > needReschedule_;
    boost::mutex jobsMut_;
};

} // namespace master

#endif

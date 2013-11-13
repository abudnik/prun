#ifndef __SCHEDULER_H
#define __SCHEDULER_H

#include <set>
#include <map>
#include <list>
#include <boost/thread/mutex.hpp>
#include "common/observer.h"
#include "worker.h"
#include "job.h"
#include "failed_workers.h"
#include "scheduled_jobs.h"
#include "node_state.h"
#include "worker_priority.h"


namespace master {

class Scheduler : public common::Observable< true >
{
private:
    typedef std::map< std::string, NodeState > IPToNodeState;
    typedef std::list< WorkerTask > TaskList;
    typedef std::map< int64_t, std::set< int > > JobIdToTasks; // job_id -> set(task_id)

private:
    Scheduler();

public:
    void OnHostAppearance( Worker *worker );

    void OnChangedWorkerState( const std::vector< Worker * > &workers );

    void OnNewJob();

    bool GetTaskToSend( WorkerJob &workerJob, std::string &hostIP, Job **job );

    void OnTaskSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const Job *job );

    void OnTaskCompletion( int errCode, const WorkerTask &workerTask, const std::string &hostIP );

    void OnTaskTimeout( const WorkerTask &workerTask, const std::string &hostIP );
    void OnJobTimeout( int64_t jobId );

    void StopJob( int64_t jobId );
    void StopJobGroup( int64_t groupId );

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
    bool GetJobForWorker( const Worker *worker, WorkerJob &workerJob, Job **job, int numCPU );

    void OnRemoveJob( int64_t jobId );
    void StopWorkers( int64_t jobId );

    bool CanTakeNewJob() const;

    int GetNumPlannedExec( const Job *job ) const;

    // stats
    int GetNumBusyWorkers() const;
    int GetNumFreeWorkers() const;
    int GetNumBusyCPU() const;

private:
    IPToNodeState nodeState_;
    WorkerPriority workerPriority_;
    FailedWorkers failedWorkers_;
    boost::mutex workersMut_;

    ScheduledJobs jobs_;
    JobIdToTasks tasksToSend_;
    TaskList needReschedule_;
    boost::mutex jobsMut_;
};

} // namespace master

#endif

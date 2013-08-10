#ifndef __SHEDULER_H
#define __SHEDULER_H

#include <set>
#include <queue>
#include <boost/thread/mutex.hpp>
#include "common/observer.h"
#include "worker.h"
#include "job.h"


namespace master {

class Sheduler : public python_server::Observable< true >
{
public:
    void OnHostAppearance( Worker *worker );

    void OnChangedWorkerState( const std::vector< Worker * > &workers );

    void OnNewJob( Job *job );

    void OnTaskCompletion( const Worker *worker );

	bool GetTaskToSend( Worker **worker, Job **job );

    void OnTaskSendCompletion( bool success, const Worker *worker, const Job *job );

    static Sheduler &Instance()
    {
        static Sheduler instance_;
        return instance_;
    }

	void Shutdown();

private:
	void PlanJobExecution();

    bool CheckIfWorkerFailedJob( Worker *worker, int64_t jobId ) const;
    bool CanTakeNewJob() const;
    bool NeedToSendTask() const;

private:
	IPToWorker busyWorkers_, freeWorkers_, sendingJobWorkers_;
	std::map< int64_t, std::set< std::string > > failedWorkers_; // job_id -> set(worker_ip)
    boost::mutex workersMut_;

    std::queue< Job * > jobs_;
	std::map< int64_t, int > jobExecutions_; // job_id -> num job remaining executions (== 0, if job execution completed)
	std::map< int64_t, std::set< int > > tasksToSend_; // job_id -> set(task_id)
	std::queue< WorkerJob > needReschedule_;
    boost::mutex jobsMut_;
};

} // namespace master

#endif

#ifndef __SHEDULER_H
#define __SHEDULER_H

#include <set>
#include <list>
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

	bool GetTaskToSend( WorkerJob &workerJob, std::string &hostIP, Job **job );

    void OnTaskSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const Job *job );

    void OnTaskCompletion( int errCode, const WorkerJob &workerJob, const std::string &hostIP );

    static Sheduler &Instance()
    {
        static Sheduler instance_;
        return instance_;
    }

    void PrintStats(); // dbg only?

	void Shutdown();

private:
	void PlanJobExecution();

    void RemoveJob( int64_t jobId );

    bool CheckIfWorkerFailedJob( Worker *worker, int64_t jobId ) const;
    bool CanTakeNewJob() const;
    bool NeedToSendTask() const;

private:
	IPToWorker busyWorkers_, freeWorkers_, sendingJobWorkers_;
	std::map< int64_t, std::set< std::string > > failedWorkers_; // job_id -> set(worker_ip)
    boost::mutex workersMut_;

    std::list< Job * > jobs_;
	std::map< int64_t, int > jobExecutions_; // job_id -> num job remaining executions (== 0, if job execution completed)
	std::map< int64_t, std::set< int > > tasksToSend_; // job_id -> set(task_id)
	std::list< WorkerJob > needReschedule_;
    boost::mutex jobsMut_;
};

} // namespace master

#endif

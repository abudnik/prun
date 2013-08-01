#ifndef __SHEDULER_H
#define __SHEDULER_H

#include <set>
#include <queue>
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

    void OnTaskCompletion(/*args*/);

    void OnTaskSend( bool success /*args*/);

	void GetTaskToSend( Worker **worker, Job **job );

    static Sheduler &Instance()
    {
        static Sheduler instance_;
        return instance_;
    }

private:
    bool CanTakeNewJob() const;

private:
	IPToWorker busyWorkers, freeWorkers, sendingJobWorkers;

	std::map< int64_t, std::set< std::string > > failedWorkers; // job_id -> set(worker_ip)

	std::map< Job *, int > jobs_; // job -> numJobExecutions (== 0, if job execution completed)
	std::map< int64_t, std::set< int > > tasksToSend_; // job_id -> set(task_id)
	std::queue< WorkerJob > needReschedule_;
};

} // namespace master

#endif

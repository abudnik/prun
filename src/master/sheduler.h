#ifndef __SHEDULER_H
#define __SHEDULER_H

#include <set>
#include <queue>
#include "worker.h"


namespace master {

class Sheduler
{
public:
    void OnHostAppearance( Worker *worker );

    void OnChangedWorkerState( const std::vector< Worker * > &workers );

    static Sheduler &Instance()
    {
        static Sheduler instance_;
        return instance_;
    }

private:
	IPToWorker busyWorkers, freeWorkers;

	std::map< int64_t, std::set< std::string > > failedWorkers; // job_id -> set(worker_ip)

	std::queue< WorkerJob > needReschedule_;
};

} // namespace master

#endif

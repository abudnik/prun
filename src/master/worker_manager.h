#ifndef __WORKER_MANAGER_H
#define __WORKER_MANAGER_H

#include <list>
#include <queue>
#include <utility> // pair
#include <boost/thread/mutex.hpp>
#include "common/observer.h"
#include "worker.h"

namespace master {

class WorkerManager : public python_server::Observable< true >
{
public:
    template< class Container >
    void Initialize( const Container &hosts )
    {
        typename Container::const_iterator it = hosts.begin();
        for( ; it != hosts.end(); ++it )
        {
            workers_.AddWorker( new Worker( (const std::string &)( *it ) ) );
        }
    }

    void CheckDropedPingResponses();

    void OnNodePingResponse( const std::string &hostIP );

    void OnNodeJobCompletion( const std::string &hostIP, int64_t jobId, int taskId );

    bool GetAchievedWorker( WorkerJob &worker, std::string &hostIP );

    void SetWorkerIP( Worker *worker, const std::string &ip );
    Worker *GetWorkerByIP( const std::string &ip ) const;

    WorkerList::WorkerContainer &GetWorkers() { return workers_.GetWorkers(); }
    int GetTotalWorkers() { return workers_.GetTotalWorkers(); }

    static WorkerManager &Instance()
    {
        static WorkerManager instance_;
        return instance_;
    }

    void Shutdown();

private:
    WorkerList workers_;
    std::queue< std::pair< WorkerJob, std::string > > achievedWorkers_;
    boost::mutex workersMut_;
};

bool ReadHosts( const char *filePath, std::list< std::string > &hosts );

} // namespace master

#endif

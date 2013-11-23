#ifndef __WORKER_MANAGER_H
#define __WORKER_MANAGER_H

#include <list>
#include <queue>
#include <utility> // pair
#include <boost/thread/mutex.hpp>
#include "common/observer.h"
#include "worker.h"
#include "command.h"

namespace master {

class WorkerManager : public common::Observable< true >
{
    typedef std::map< std::string, WorkerList > GrpNameToWorkerList;

public:
    enum ObserverEvent { eTaskCompletion, eCommand };

public:
    void AddWorkerGroup( const std::string &groupName, std::list< std::string > &hosts );
    void AddWorkerHost( const std::string &groupName, const std::string &host );

    void CheckDropedPingResponses();

    void OnNodePingResponse( const std::string &hostIP, int numCPU );

    void OnNodeTaskCompletion( const std::string &hostIP, int64_t jobId, int taskId );

    bool GetAchievedTask( WorkerTask &worker, std::string &hostIP );

    void SetWorkerIP( Worker *worker, const std::string &ip );
    Worker *GetWorkerByIP( const std::string &ip ) const;

    void AddCommand( CommandPtr &command, const std::string &hostIP );
    bool GetCommand( CommandPtr &command, std::string &hostIP );

    template< typename Container >
    void GetWorkers( Container &workers ) const
    {
        GrpNameToWorkerList::const_iterator it = workerGroups_.begin();
        for( ; it != workerGroups_.end(); ++it )
        {
            const WorkerList &workerList = it->second;
            const WorkerList::WorkerContainer &w = workerList.GetWorkers();
            WorkerList::WorkerContainer::const_iterator w_it = w.begin();
            for( ; w_it != w.end(); ++w_it )
            {
                workers.push_back( *w_it );
            }
        }
    }

    int GetTotalWorkers() const;
    int GetTotalCPU() const;

    static WorkerManager &Instance()
    {
        static WorkerManager instance_;
        return instance_;
    }

    void Shutdown();

private:
    GrpNameToWorkerList workerGroups_;
    typedef std::pair< WorkerTask, std::string > PairTypeAW;
    std::queue< PairTypeAW > achievedWorkers_;
    boost::mutex workersMut_;
    typedef std::pair< CommandPtr, std::string > PairTypeC;
    std::queue< PairTypeC > commands_;
    boost::mutex commandsMut_;
};

bool ReadHosts( const char *filePath, std::list< std::string > &hosts );

} // namespace master

#endif

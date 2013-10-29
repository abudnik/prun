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

class WorkerManager : public python_server::Observable< true >
{
public:
    enum ObserverEvent { eTaskCompletion, eCommand };

public:
    template< class InputIterator >
    void Initialize( InputIterator first, InputIterator last )
    {
        InputIterator it = first;
        for( ; it != last; ++it )
        {
            workers_.AddWorker( new Worker( (const std::string &)( *it ) ) );
        }
    }

    void CheckDropedPingResponses();

    void OnNodePingResponse( const std::string &hostIP, int numCPU );

    void OnNodeTaskCompletion( const std::string &hostIP, int64_t jobId, int taskId );

    bool GetAchievedTask( WorkerTask &worker, std::string &hostIP );

    void SetWorkerIP( Worker *worker, const std::string &ip );
    Worker *GetWorkerByIP( const std::string &ip ) const;

    void AddCommand( CommandPtr &command, const std::string &hostIP );
    bool GetCommand( CommandPtr &command, std::string &hostIP );
    void OnCommandCompletion( int errCode, CommandPtr &command, const std::string &hostIP );

    WorkerList::WorkerContainer &GetWorkers() { return workers_.GetWorkers(); }
    int GetTotalWorkers() const { return workers_.GetTotalWorkers(); }
    int GetTotalCPU() const { return workers_.GetTotalCPU(); }

    static WorkerManager &Instance()
    {
        static WorkerManager instance_;
        return instance_;
    }

    void Shutdown();

private:
    WorkerList workers_;
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

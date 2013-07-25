#ifndef __WORKER_MANAGER_H
#define __WORKER_MANAGER_H

#include <list>
#include "worker.h"

namespace master {

class WorkerManager
{
public:
    template< class Container >
    void Initialize( const Container &hosts )
    {
        typename Container::const_iterator it = hosts.begin();
        for( ; it != hosts.end(); ++it )
        {
            workers_.AddWorker( new Worker( (*it).c_str() ) );
        }
    }

    void Shutdown()
    {
        workers_.Clear();
    }

    void CheckDropedPingResponses();

    void OnHostPingResponse( const std::string &host );

	void SetHostIP( const std::string &ip ) { hostIP_ = ip; }
	const std::string &GetHostIP() const { return hostIP_; }

    WorkerList::WorkerContainer &GetWorkers() { return workers_.GetWorkers(); }

    static WorkerManager &Instance()
    {
        static WorkerManager instance_;
        return instance_;
    }

private:
    WorkerList workers_;
	std::string hostIP_;
};

bool ReadHosts( const char *filePath, std::list< std::string > &hosts );

} // namespace master

#endif

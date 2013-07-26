#ifndef __WORKER_H
#define __WORKER_H

#include <vector>
#include <map>
#include <string>

namespace master {

enum WorkerState
{
    WORKER_STATE_NOT_AVAIL = 1,
    WORKER_STATE_READY     = 2,
    WORKER_STATE_EXEC      = 4,
    WORKER_STATE_FAILED    = 8
};

class Worker
{
public:
    Worker( const std::string &host )
    : host_( host ), state_( WORKER_STATE_NOT_AVAIL ),
     numPingResponse_( 0 )
    {}

    Worker()
    : state_( WORKER_STATE_NOT_AVAIL ),
     numPingResponse_( 0 )
    {}

    void SetHost( const std::string &host ) { host_ = host; }
    void SetIP( const std::string &ip ) { ip_ = ip; }
    void SetState( WorkerState state ) { state_ = state; }
    void SetNumPingResponse( int num ) { numPingResponse_ = num; }
    void IncNumPingResponse() { ++numPingResponse_; }

	const std::string &GetHost() const { return host_; }
	const std::string &GetIP() const { return ip_; }
	WorkerState GetState() const { return state_; }
    int GetNumPingResponse() const { return numPingResponse_; }

private:
    std::string host_;
    std::string ip_;
    WorkerState state_;
    int numPingResponse_;
};

class WorkerList
{
    typedef std::map< std::string, Worker * > IPToWorker;

public:
    typedef std::vector< Worker* > WorkerContainer;

public:
    void AddWorker( Worker *worker );

    void Clear( bool doDelete = true );

    Worker *GetWorker( const char *host ) const;

    void SetWorkerIP( Worker *worker, const std::string &ip );
    Worker *GetWorkerByIP( const std::string &ip ) const;

    template< class Container >
    void GetWorkerList( Container &workers, int stateMask ) const
    {
        WorkerContainer::const_iterator it = workers_.begin();
        for( ; it != workers_.end(); ++it )
        {
            int state = (int)(*it)->GetState();
            if ( state & stateMask )
            {
                workers.push_back( *it );
            }
        }
    }

    WorkerContainer &GetWorkers() { return workers_; }

    int GetTotalWorkers() const { return workers_.size(); }

    int GetNumWorkers( int stateMask ) const;

private:
    WorkerContainer workers_;
    IPToWorker ipToWorker_;
};

} // namespace master

#endif

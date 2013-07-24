#ifndef __WORKER_H
#define __WORKER_H

#include <vector>
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
    Worker( const char *host )
    : host_( host ), state_( WORKER_STATE_NOT_AVAIL )
    {}

    Worker()
    : state_( WORKER_STATE_NOT_AVAIL )
    {}

    void SetHost( const char *host ) { host_ = host; }
    void SetState( WorkerState state ) { state_ = state; }

	const std::string &GetHost() const { return host_; }
	WorkerState GetState() const { return state_; }

private:
    std::string host_;
    WorkerState state_;    
};

class WorkerList
{
public:
    typedef std::vector< Worker* > WorkerContainer;

public:
    void AddWorker( Worker *worker );

    Worker *RemoveWorker( const char *host );

    void Clear( bool doDelete = true );

    Worker *GetWorker( const char *host ) const;

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
};

} // namespace master

#endif

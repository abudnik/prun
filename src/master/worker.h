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
    typedef std::vector< Worker* > WorkerContainer;

public:
    WorkerList()
    : numWorkers_( 0 )
    {}

    void AddWorker( Worker *worker )
    {
        workers_.push_back( worker );
        ++numWorkers_;
    }

    void RemoveWorker( const char *host )
    {
        WorkerContainer::iterator it = workers_.begin();
        for( ; it != workers_.end(); ++it )
        {
            if ( (*it)->GetHost() == host )
            {
                workers_.erase( it );
                --numWorkers_;
                break;
            }
        }
    }

    void Clear()
    {
        workers_.clear();
        numWorkers_ = 0;
    }

    Worker *GetWorker( const char *host ) const
    {
        WorkerContainer::const_iterator it = workers_.begin();
        for( ; it != workers_.end(); ++it )
        {
            if ( (*it)->GetHost() == host )
                return *it;
        }
        return NULL;
    }

    template< template< class W > class List >
    void GetWorkerList( List< Worker * > &list, int stateMask ) const
    {
        WorkerContainer::const_iterator it = workers_.begin();
        for( ; it != workers_.end(); ++it )
        {
            int state = (int)(*it)->GetState();
            if ( state & stateMask )
            {
                list.push_back( *it );
            }
        }
    }

private:
    WorkerContainer workers_;
    int numWorkers_;
};

} // namespace master

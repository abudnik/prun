#include "worker.h"

namespace master {

void WorkerList::AddWorker( Worker *worker )
{
    workers_.push_back( worker );
    ++numWorkers_;
}

Worker *WorkerList::RemoveWorker( const char *host )
{
    WorkerContainer::iterator it = workers_.begin();
    for( ; it != workers_.end(); ++it )
    {
        if ( (*it)->GetHost() == host )
        {
            Worker *w = *it;
            workers_.erase( it );
            --numWorkers_;
            return w;
        }
    }
    return NULL;
}

void WorkerList::Clear( bool doDelete )
{
    if ( doDelete )
    {
        WorkerContainer::iterator it = workers_.begin();
        for( ; it != workers_.end(); ++it )
        {
            delete *it;
        }
    }
    workers_.clear();
    numWorkers_ = 0;
}

Worker *WorkerList::GetWorker( const char *host ) const
{
    WorkerContainer::const_iterator it = workers_.begin();
    for( ; it != workers_.end(); ++it )
    {
        if ( (*it)->GetHost() == host )
            return *it;
    }
    return NULL;
}

} // namespace master

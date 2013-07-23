#include "worker.h"

namespace master {

void WorkerList::AddWorker( Worker *worker )
{
    workers_.push_back( worker );
    ++numWorkers_;
}

void WorkerList::RemoveWorker( const char *host )
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

void WorkerList::Clear()
{
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

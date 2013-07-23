#include "worker.h"

namespace master {

void WorkerList::AddWorker( Worker *worker )
{
    workers_.push_back( worker );
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

int WorkerList::GetNumWorkers( int stateMask ) const
{
    int num = 0;
    WorkerContainer::const_iterator it = workers_.begin();
    for( ; it != workers_.end(); ++it )
    {
        int state = (int)(*it)->GetState();
        if ( state & stateMask )
            ++num;
    }
    return num;
}

} // namespace master

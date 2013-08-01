#include "worker.h"

namespace master {

void WorkerList::AddWorker( Worker *worker )
{
    workers_.push_back( worker );
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

void WorkerList::SetWorkerIP( Worker *worker, const std::string &ip )
{
    worker->SetIP( ip );
    ipToWorker_[ip] = worker;
}

Worker *WorkerList::GetWorkerByIP( const std::string &ip ) const
{
    IPToWorker::const_iterator it = ipToWorker_.find( ip );
    if ( it != ipToWorker_.end() )
        return it->second;
    return NULL;
}

int WorkerList::GetTotalWorkers() const
{
    int num = 0;
    WorkerContainer::const_iterator it = workers_.begin();
    for( ; it != workers_.end(); ++it )
    {
		num += (*it)->GetNumCores();
    }
    return num;
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

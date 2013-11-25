#include "worker.h"

namespace master {

void WorkerJob::AddTask( int64_t jobId, int taskId )
{
    jobs_[ jobId ].insert( taskId );
}

bool WorkerJob::DeleteTask( int64_t jobId, int taskId )
{
    JobIdToTasks::iterator it = jobs_.find( jobId );
    if ( it != jobs_.end() )
    {
        Tasks &tasks = it->second;
        Tasks::iterator it_tasks = tasks.find( taskId );
        if ( it_tasks != tasks.end() )
        {
            tasks.erase( it_tasks );
            if ( tasks.empty() )
                jobs_.erase( it );
            return true;
        }
    }
    return false;
}

bool WorkerJob::DeleteJob( int64_t jobId )
{
    JobIdToTasks::iterator it = jobs_.find( jobId );
    if ( it != jobs_.end() )
    {
        jobs_.erase( it );
        return true;
    }
    return false;
}

bool WorkerJob::HasTask( int64_t jobId, int taskId ) const
{
    JobIdToTasks::const_iterator it = jobs_.find( jobId );
    if ( it != jobs_.end() )
    {
        const Tasks &tasks = it->second;
        Tasks::const_iterator it_tasks = tasks.find( taskId );
        return it_tasks != tasks.end();
    }
    return false;
}

bool WorkerJob::HasJob( int64_t jobId ) const
{
    return jobs_.find( jobId ) != jobs_.end();
}

bool WorkerJob::GetTasks( int64_t jobId, Tasks &tasks ) const
{
    JobIdToTasks::const_iterator it = jobs_.find( jobId );
    if ( it != jobs_.end() )
    {
        tasks = it->second;
        return true;
    }
    return false;
}

void WorkerJob::GetTasks( std::vector< WorkerTask > &tasks ) const
{
    JobIdToTasks::const_iterator it = jobs_.begin();
    for( ; it != jobs_.end(); ++it )
    {
        const Tasks &t = it->second;
        Tasks::const_iterator it_tasks = t.begin();
        for( ; it_tasks != t.end(); ++it_tasks )
        {
            int taskId = *it_tasks;
            tasks.push_back( WorkerTask( it->first, taskId ) );
        }
    }
}

void WorkerJob::GetJobs( std::set<int64_t> &jobs ) const
{
    JobIdToTasks::const_iterator it = jobs_.begin();
    for( ; it != jobs_.end(); ++it )
    {
        jobs.insert( it->first );
    }
}

int64_t WorkerJob::GetJobId() const
{
    if ( jobs_.empty() )
        return -1;
    JobIdToTasks::const_iterator it = jobs_.begin();
    return it->first;
}

int WorkerJob::GetNumJobs() const
{
    return jobs_.size();
}

int WorkerJob::GetTotalNumTasks() const
{
    int num = 0;
    JobIdToTasks::const_iterator it = jobs_.begin();
    for( ; it != jobs_.end(); ++it )
    {
        const Tasks &tasks = it->second;
        num += (int)tasks.size();
    }
    return num;
}

int WorkerJob::GetNumTasks( int64_t jobId ) const
{
    JobIdToTasks::const_iterator it = jobs_.find( jobId );
    if ( it != jobs_.end() )
    {
        const Tasks &tasks = it->second;
        return (int)tasks.size();
    }
    return 0;
}

WorkerJob &WorkerJob::operator += ( const WorkerJob &workerJob )
{
    std::vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    std::vector< WorkerTask >::const_iterator it = tasks.begin();
    for( ; it != tasks.end(); ++it )
    {
        const WorkerTask &task = *it;
        AddTask( task.GetJobId(), task.GetTaskId() );
    }
    return *this;
}

void WorkerJob::Reset()
{
    jobs_.clear();
}


void WorkerList::AddWorker( Worker *worker )
{
    workers_.push_back( WorkerPtr( worker ) );
}

void WorkerList::DeleteWorker( const std::string &host )
{
    WorkerContainer::iterator it = workers_.begin();
    for( ; it != workers_.end(); )
    {
        WorkerPtr &w = *it;
        if ( w->GetHost() == host )
        {
            w->SetState( WORKER_STATE_DISABLED );
            workers_.erase( it++ );
        }
        else
            ++it;
    }
}

void WorkerList::Clear()
{
    WorkerContainer::iterator it = workers_.begin();
    for( ; it != workers_.end(); )
    {
        WorkerPtr &w = *it;
        w->SetState( WORKER_STATE_DISABLED );
    }

    workers_.clear();
}

bool WorkerList::GetWorker( const char *host, WorkerPtr &worker )
{
    WorkerContainer::iterator it = workers_.begin();
    for( ; it != workers_.end(); ++it )
    {
        if ( (*it)->GetHost() == host )
        {
            worker = *it;
            return true;
        }
    }
    return false;
}

bool WorkerList::SetWorkerIP( WorkerPtr &worker, const std::string &ip )
{
    WorkerContainer::const_iterator it = workers_.begin();
    for( ; it != workers_.end(); ++it )
    {
        if ( worker == *it )
        {
            worker->SetIP( ip );
            ipToWorker_[ip] = worker;
            return true;
        }
    }
    return false;
}

bool WorkerList::GetWorkerByIP( const std::string &ip, WorkerPtr &worker ) const
{
    IPToWorker::const_iterator it = ipToWorker_.find( ip );
    if ( it != ipToWorker_.end() )
    {
        worker = it->second;
        return true;
    }
    return false;
}

int WorkerList::GetTotalWorkers() const
{
    int num = 0;
    WorkerContainer::const_iterator it = workers_.begin();
    for( ; it != workers_.end(); ++it )
    {
        ++num;
    }
    return num;
}

int WorkerList::GetTotalCPU() const
{
    int num = 0;
    WorkerContainer::const_iterator it = workers_.begin();
    for( ; it != workers_.end(); ++it )
    {
        num += (*it)->GetNumCPU();
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
        {
            ++num;
        }
    }
    return num;
}

int WorkerList::GetNumCPU( int stateMask ) const
{
    int num = 0;
    WorkerContainer::const_iterator it = workers_.begin();
    for( ; it != workers_.end(); ++it )
    {
        int state = (int)(*it)->GetState();
        if ( state & stateMask )
        {
            num += (*it)->GetNumCPU();
        }
    }
    return num;
}

} // namespace master

/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2013 Andrey Budnik <budnik27@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

===========================================================================
*/

#include "worker.h"

namespace master {

void WorkerJob::AddTask( int64_t jobId, int taskId )
{
    jobs_[ jobId ].insert( taskId );
}

bool WorkerJob::DeleteTask( int64_t jobId, int taskId )
{
    auto it = jobs_.find( jobId );
    if ( it != jobs_.end() )
    {
        Tasks &tasks = it->second;
        auto it_tasks = tasks.find( taskId );
        if ( it_tasks != tasks.end() )
        {
            tasks.erase( it_tasks );
            if ( tasks.empty() )
            {
                jobs_.erase( it );
                if ( IsExclusive() && jobs_.empty() )
                {
                    SetExclusive( false );
                }
            }
            return true;
        }
    }
    return false;
}

bool WorkerJob::DeleteJob( int64_t jobId )
{
    auto it = jobs_.find( jobId );
    if ( it != jobs_.end() )
    {
        jobs_.erase( it );
        if ( IsExclusive() && jobs_.empty() )
        {
            SetExclusive( false );
        }
        return true;
    }
    return false;
}

bool WorkerJob::HasTask( int64_t jobId, int taskId ) const
{
    auto it = jobs_.find( jobId );
    if ( it != jobs_.end() )
    {
        const Tasks &tasks = it->second;
        auto it_tasks = tasks.find( taskId );
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
    auto it = jobs_.find( jobId );
    if ( it != jobs_.end() )
    {
        tasks = it->second;
        return true;
    }
    return false;
}

void WorkerJob::GetTasks( std::vector< WorkerTask > &tasks ) const
{
    for( auto it = jobs_.cbegin(); it != jobs_.cend(); ++it )
    {
        const Tasks &t = it->second;
        Tasks::const_iterator it_tasks = t.begin();
        for( ; it_tasks != t.end(); ++it_tasks )
        {
            int taskId = *it_tasks;
            tasks.emplace_back( it->first, taskId );
        }
    }
}

void WorkerJob::GetJobs( std::set<int64_t> &jobs ) const
{
    for( auto it = jobs_.cbegin(); it != jobs_.cend(); ++it )
    {
        jobs.insert( it->first );
    }
}

int64_t WorkerJob::GetJobId() const
{
    if ( jobs_.empty() )
        return -1;
    auto it = jobs_.cbegin();
    return it->first;
}

int WorkerJob::GetNumJobs() const
{
    return jobs_.size();
}

int WorkerJob::GetTotalNumTasks() const
{
    int num = 0;
    for( auto it = jobs_.cbegin(); it != jobs_.cend(); ++it )
    {
        const Tasks &tasks = it->second;
        num += static_cast<int>( tasks.size() );
    }
    return num;
}

int WorkerJob::GetNumTasks( int64_t jobId ) const
{
    auto it = jobs_.find( jobId );
    if ( it != jobs_.end() )
    {
        const Tasks &tasks = it->second;
        return static_cast<int>( tasks.size() );
    }
    return 0;
}

WorkerJob &WorkerJob::operator += ( const WorkerJob &workerJob )
{
    std::vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    for( const auto &task : tasks )
    {
        AddTask( task.GetJobId(), task.GetTaskId() );
    }
    if ( workerJob.IsExclusive() )
    {
        SetExclusive( true );
    }
    return *this;
}

void WorkerJob::SetExclusive( bool exclusive )
{
    exclusive_ = exclusive;
}

bool WorkerJob::IsExclusive() const
{
    return exclusive_;
}

void WorkerJob::Reset()
{
    jobs_.clear();
    exclusive_ = false;
}


void WorkerList::AddWorker( Worker *worker )
{
    workers_.emplace_back( worker );
}

void WorkerList::DeleteWorker( const std::string &host )
{
    for( auto it = workers_.begin(); it != workers_.end(); )
    {
        WorkerPtr &w = *it;
        if ( w->GetHost() == host )
        {
            w->SetState( WORKER_STATE_DISABLED );

            ipToWorker_.erase( w->GetIP() );
            it = workers_.erase( it );
        }
        else
            ++it;
    }
}

void WorkerList::Clear()
{
    for( auto &w : workers_ )
    {
        w->SetState( WORKER_STATE_DISABLED );
    }

    workers_.clear();
    ipToWorker_.clear();
}

bool WorkerList::GetWorker( const char *host, WorkerPtr &worker )
{
    for( auto &w : workers_ )
    {
        if ( w->GetHost() == host )
        {
            worker = w;
            return true;
        }
    }
    return false;
}

bool WorkerList::SetWorkerIP( WorkerPtr &worker, const std::string &ip )
{
    for( const auto &w : workers_ )
    {
        if ( worker == w )
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
    auto it = ipToWorker_.find( ip );
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
    for( const auto &worker : workers_ )
    {
        if ( worker->IsAvailable() )
        {
            ++num;
        }
    }
    return num;
}

int WorkerList::GetTotalCPU() const
{
    int num = 0;
    for( const auto &worker : workers_ )
    {
        if ( worker->IsAvailable() )
        {
            num += worker->GetNumCPU();
        }
    }
    return num;
}

int WorkerList::GetNumWorkers( int stateMask ) const
{
    int num = 0;
    for( const auto &worker : workers_ )
    {
        int state = static_cast<int>( worker->GetState() );
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
    for( const auto &worker : workers_ )
    {
        int state = static_cast<int>( worker->GetState() );
        if ( state & stateMask )
        {
            num += worker->GetNumCPU();
        }
    }
    return num;
}

} // namespace master

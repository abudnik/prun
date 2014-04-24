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

#ifndef __WORKER_H
#define __WORKER_H

#include <vector>
#include <map>
#include <set>
#include <string>
#include <stdint.h> // int64_t
#include <boost/shared_ptr.hpp>

namespace master {

enum WorkerState
{
    WORKER_STATE_NOT_AVAIL = 1,
    WORKER_STATE_READY     = 2,
    WORKER_STATE_EXEC      = 4,
    WORKER_STATE_DISABLED  = 8
};

class WorkerTask
{
public:
    WorkerTask( int64_t jobId, int taskId )
    : jobId_( jobId ), taskId_( taskId ) {}
    WorkerTask() : jobId_( -1 ), taskId_( -1 ) {}

    int64_t GetJobId() const { return jobId_; }
    int GetTaskId() const { return taskId_; }

private:
    int64_t jobId_;
    int taskId_;
};

class WorkerJob
{
public:
    typedef std::set<int> Tasks;

private:
    typedef std::map< int64_t, Tasks > JobIdToTasks;

public:
    WorkerJob() : exclusive_( false ) {}

    void AddTask( int64_t jobId, int taskId );

    bool DeleteTask( int64_t jobId, int taskId );

    bool DeleteJob( int64_t jobId );

    bool HasTask( int64_t jobId, int taskId ) const;

    bool HasJob( int64_t jobId ) const;

    bool GetTasks( int64_t jobId, Tasks &tasks ) const;

    void GetTasks( std::vector< WorkerTask > &tasks ) const;

    void GetJobs( std::set<int64_t> &jobs ) const;

    int64_t GetJobId() const;

    int GetNumJobs() const;

    int GetTotalNumTasks() const;

    int GetNumTasks( int64_t jobId ) const;

    WorkerJob &operator += ( const WorkerJob &workerJob );

    void SetExclusive( bool exclusive );

    bool IsExclusive() const;

    void Reset();

private:
    JobIdToTasks jobs_;
    bool exclusive_;
};


class Worker
{
public:
    Worker( const std::string &host, const std::string &group )
    : host_( host ), group_( group ),
     state_( WORKER_STATE_NOT_AVAIL ),
     numCPU_( 0 ), numPingResponse_( 0 )
    {}

    Worker()
    : state_( WORKER_STATE_NOT_AVAIL ),
     numCPU_( 0 ), numPingResponse_( 0 )
    {}

    void SetHost( const std::string &host ) { host_ = host; }
    void SetGroup( const std::string &group ) { group_ = group; }
    void SetIP( const std::string &ip ) { ip_ = ip; }
    void SetNumCPU( int numCPU ) { numCPU_ = numCPU; }
    void SetMemorySize( int64_t memSizeMb ) { memSizeMb_ = memSizeMb; }
    void SetState( WorkerState state ) { state_ = state; }
    void SetJob( const WorkerJob &job ) { job_ = job; }
    void ResetJob() { job_.Reset(); }
    void SetNumPingResponse( int num ) { numPingResponse_ = num; }
    void IncNumPingResponse() { ++numPingResponse_; }

    const std::string &GetHost() const { return host_; }
    const std::string &GetGroup() const { return group_; }
    const std::string &GetIP() const { return ip_; }
    int GetNumCPU() const { return numCPU_; }
    int64_t GetMemorySize() const { return memSizeMb_; }
    WorkerState GetState() const { return state_; }
    const WorkerJob &GetJob() const { return job_; }
    WorkerJob &GetJob() { return job_; }
    int GetNumPingResponse() const { return numPingResponse_; }

    bool IsAvailable() const
    { return state_ != WORKER_STATE_NOT_AVAIL &&
             state_ != WORKER_STATE_DISABLED; }

private:
    std::string host_, group_;
    std::string ip_;
    WorkerState state_;
    WorkerJob job_;
    int numCPU_;
    int64_t memSizeMb_;
    int numPingResponse_;
};

typedef boost::shared_ptr< Worker > WorkerPtr;
typedef std::map< std::string, WorkerPtr > IPToWorker;

class WorkerList
{
public:
    typedef std::vector< WorkerPtr > WorkerContainer;

public:
    void AddWorker( Worker *worker );

    void DeleteWorker( const std::string &host );

    void Clear();

    bool GetWorker( const char *host, WorkerPtr &worker );

    bool SetWorkerIP( WorkerPtr &worker, const std::string &ip );
    bool GetWorkerByIP( const std::string &ip, WorkerPtr &worker ) const;

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
    const WorkerContainer &GetWorkers() const { return workers_; }

    int GetTotalWorkers() const;
    int GetTotalCPU() const;

    int GetNumWorkers( int stateMask ) const;
    int GetNumCPU( int stateMask ) const;

private:
    WorkerContainer workers_;
    IPToWorker ipToWorker_;
};

} // namespace master

#endif

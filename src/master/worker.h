#ifndef __WORKER_H
#define __WORKER_H

#include <vector>
#include <map>
#include <set>
#include <string>
#include <stdint.h> // int64_t

namespace master {

enum WorkerState
{
    WORKER_STATE_NOT_AVAIL = 1,
    WORKER_STATE_READY     = 2,
    WORKER_STATE_EXEC      = 4,
    WORKER_STATE_FAILED    = 8
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
    void Reset()
    {
        jobs_.clear();
    }

    void AddTask( int64_t jobId, int taskId ) { jobs_[ jobId ].insert( taskId ); }
    bool DeleteTask( int64_t jobId, int taskId )
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

    bool HasTask( int64_t jobId, int taskId ) const
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

    bool HasJob( int64_t jobId ) const
    {
        return jobs_.find( jobId ) != jobs_.end();
    }

    bool GetTasks( int64_t jobId, Tasks &tasks ) const
    {
        JobIdToTasks::const_iterator it = jobs_.find( jobId );
        if ( it != jobs_.end() )
        {
            tasks = it->second;
            return true;
        }
        return false;
    }

    void GetTasks( std::vector< WorkerTask > &tasks ) const
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

    int GetNumTasks() const
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

    WorkerJob &operator += ( const WorkerJob &workerJob )
    {
        std::vector< WorkerTask > tasks;
        GetTasks( tasks );
        std::vector< WorkerTask >::const_iterator it = tasks.begin();
        for( ; it != tasks.end(); ++it )
        {
            const WorkerTask &task = *it;
            AddTask( task.GetJobId(), task.GetTaskId() );
        }
        return *this;
    }

private:
    JobIdToTasks jobs_;
};


class Worker
{
public:
    Worker( const std::string &host )
    : host_( host ),
     state_( WORKER_STATE_NOT_AVAIL ),
     numCPU_( 1 ), numPingResponse_( 0 )
    {}

    Worker()
    : state_( WORKER_STATE_NOT_AVAIL ),
     numCPU_( 1 ), numPingResponse_( 0 )
    {}

    void SetHost( const std::string &host ) { host_ = host; }
    void SetIP( const std::string &ip ) { ip_ = ip; }
    void SetNumCPU( int numCPU ) { numCPU_ = numCPU; }
    void SetState( WorkerState state ) { state_ = state; }
    void SetJob( const WorkerJob &job ) { job_ = job; }
    void ResetJob() { job_.Reset(); }
    void SetNumPingResponse( int num ) { numPingResponse_ = num; }
    void IncNumPingResponse() { ++numPingResponse_; }

    const std::string &GetHost() const { return host_; }
    const std::string &GetIP() const { return ip_; }
    int GetNumCPU() const { return numCPU_; }
    WorkerState GetState() const { return state_; }
    const WorkerJob &GetJob() const { return job_; }
    WorkerJob &GetJob() { return job_; }
    int GetNumPingResponse() const { return numPingResponse_; }

private:
    std::string host_;
    std::string ip_;
    WorkerState state_;
    WorkerJob job_;
    int numCPU_;
    int numPingResponse_;
};

typedef std::map< std::string, Worker * > IPToWorker;

class WorkerList
{
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

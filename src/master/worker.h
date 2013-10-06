#ifndef __WORKER_H
#define __WORKER_H

#include <vector>
#include <map>
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

class WorkerJob
{
public:
    typedef std::vector<int> Tasks;

public:
    WorkerJob() : jobId_( -1 ) {}

    /*bool operator == ( const WorkerJob &workerJob ) const
    {
        return jobId_ == workerJob.jobId_ && taskId_ == workerJob.taskId_;
    }*/

    void Reset()
    {
        jobId_ = -1;
        tasks_.clear();
    }

    void SetJobId( int64_t jobId ) { jobId_ = jobId; }
    void AddTask( int taskId ) { tasks_.push_back( taskId ); }

    int64_t GetJobId() const { return jobId_; }
    const Tasks &GetTasks() const { return tasks_; }
    int GetNumTasks() const { return (int)tasks_.size(); }

private:
    int64_t jobId_;
    Tasks tasks_;
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

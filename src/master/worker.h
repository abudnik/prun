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

struct WorkerJob
{
    WorkerJob( int64_t jobId, int taskId ) : jobId_( jobId ), taskId_( taskId ) {}
    WorkerJob() : jobId_( -1 ), taskId_( -1 ) {}

    bool operator == ( const WorkerJob &workerJob ) const
    {
        return jobId_ == workerJob.jobId_ && taskId_ == workerJob.taskId_;
    }

    int64_t jobId_;
    int taskId_;
};

class Worker
{
public:
    Worker( const std::string &host )
    : host_( host ),
     state_( WORKER_STATE_NOT_AVAIL ),
     numCores_( 1 ), numPingResponse_( 0 )
    {}

    Worker()
    : state_( WORKER_STATE_NOT_AVAIL ),
     numCores_( 1 ), numPingResponse_( 0 )
    {}

    void SetHost( const std::string &host ) { host_ = host; }
    void SetIP( const std::string &ip ) { ip_ = ip; }
    void SetNumCores( int cores ) { numCores_ = cores; }
    void SetState( WorkerState state ) { state_ = state; }
    void SetJob( const WorkerJob &job ) { job_ = job; }
    void SetNumPingResponse( int num ) { numPingResponse_ = num; }
    void IncNumPingResponse() { ++numPingResponse_; }

    const std::string &GetHost() const { return host_; }
    const std::string &GetIP() const { return ip_; }
    int GetNumCores() const { return numCores_; }
    WorkerState GetState() const { return state_; }
    const WorkerJob &GetJob() const { return job_; }
    int GetNumPingResponse() const { return numPingResponse_; }

private:
    std::string host_;
    std::string ip_;
    WorkerState state_;
    WorkerJob job_;
    int numCores_;
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

    int GetNumWorkers( int stateMask ) const;

private:
    WorkerContainer workers_;
    IPToWorker ipToWorker_;
};

} // namespace master

#endif

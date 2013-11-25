#include <fstream>
#include <boost/asio/ip/address.hpp>
#include "worker_manager.h"
#include "common/log.h"
#include "scheduler.h"

namespace master {

void WorkerManager::AddWorkerGroup( const std::string &groupName, std::list< std::string > &hosts )
{
    if ( hosts.empty() )
        return;

    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    WorkerList &workerList = workerGroups_[ groupName ];
    std::list< std::string >::const_iterator it = hosts.begin();
    for( ; it != hosts.end(); ++it )
    {
        workerList.AddWorker( new Worker( (const std::string &)( *it ) ) );
    }
}

void WorkerManager::AddWorkerHost( const std::string &groupName, const std::string &host )
{
    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    WorkerList &workerList = workerGroups_[ groupName ];
    workerList.AddWorker( new Worker( host ) );
}

void WorkerManager::DeleteWorkerGroup( const std::string &groupName )
{
    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    GrpNameToWorkerList::iterator it = workerGroups_.find( groupName );
    if ( it != workerGroups_.end() )
    {
        WorkerList &workerList = it->second;
        workerList.Clear();
        workerGroups_.erase( it );
    }
}

void WorkerManager::DeleteWorkerHost( const std::string &host )
{
    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    GrpNameToWorkerList::iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        WorkerList &workerList = it->second;
        workerList.DeleteWorker( host );
    }
}

void WorkerManager::CheckDropedPingResponses()
{
    std::vector< WorkerPtr > changedWorkers;

    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    GrpNameToWorkerList::const_iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        const WorkerList &workerList = it->second;

        const WorkerList::WorkerContainer &workers = workerList.GetWorkers();
        WorkerList::WorkerContainer::const_iterator it = workers.begin();
        for( ; it != workers.end(); ++it )
        {
            const WorkerPtr &worker = *it;
            WorkerState state = worker->GetState();
            if ( !worker->GetNumPingResponse() )
            {
                if ( state == WORKER_STATE_READY )
                {
                    worker->SetState( WORKER_STATE_NOT_AVAIL );
                    changedWorkers.push_back( worker );
                    PS_LOG( "WorkerManager::CheckDropedPingResponses: node not available, ip= " << worker->GetIP() );
                }
                if ( state == WORKER_STATE_EXEC )
                {
                    worker->SetState( WORKER_STATE_NOT_AVAIL );
                    changedWorkers.push_back( worker );
                    PS_LOG( "WorkerManager::CheckDropedPingResponses: node job isn't available, ip= " << worker->GetIP() );
                }
            }
            worker->SetNumPingResponse( 0 );
        }
    }

    scoped_lock.unlock();

    if ( !changedWorkers.empty() )
    {
        Scheduler::Instance().OnChangedWorkerState( changedWorkers );
    }
}

void WorkerManager::OnNodePingResponse( const std::string &hostIP, int numCPU )
{
    WorkerPtr &worker = GetWorkerByIP( hostIP );
    if ( worker )
    {
        bool stateChanged = false;
        worker->IncNumPingResponse();
        if ( worker->GetState() == WORKER_STATE_NOT_AVAIL )
        {
            worker->SetState( WORKER_STATE_READY );
            stateChanged = true;
            PS_LOG( "node available, ip= " << worker->GetIP() );
        }

        if ( stateChanged )
        {
            worker->SetNumCPU( numCPU );
            Scheduler::Instance().OnHostAppearance( worker );
        }
    }
    else
    {
        PS_LOG( "WorkerManager::OnHostPingResponse worker not found, ip= " << hostIP );
    }
}

void WorkerManager::OnNodeTaskCompletion( const std::string &hostIP, int64_t jobId, int taskId )
{
    if ( jobId < 0 || taskId < 0 )
        return;

    PairTypeAW worker( WorkerTask( jobId, taskId ), hostIP );

    {
        boost::mutex::scoped_lock scoped_lock( achievedMut_ );
        achievedWorkers_.push( worker );
    }
    NotifyAll( eTaskCompletion );
}

bool WorkerManager::GetAchievedTask( WorkerTask &worker, std::string &hostIP )
{
    if ( achievedWorkers_.empty() )
        return false;

    boost::mutex::scoped_lock scoped_lock( achievedMut_ );
    if ( achievedWorkers_.empty() )
        return false;

    PS_LOG( "GetAchievedWorker: num achieved workers=" << achievedWorkers_.size() );

    const PairTypeAW &w = achievedWorkers_.front();
    worker = w.first;
    hostIP = w.second;
    achievedWorkers_.pop();
    return true;
}

void WorkerManager::AddCommand( CommandPtr &command, const std::string &hostIP )
{
    PairTypeC workerCommand( command, hostIP );

    {
        boost::mutex::scoped_lock scoped_lock( commandsMut_ );
        commands_.push( workerCommand );
    }
    NotifyAll( eCommand );
}

bool WorkerManager::GetCommand( CommandPtr &command, std::string &hostIP )
{
    if ( commands_.empty() )
        return false;

    boost::mutex::scoped_lock scoped_lock( commandsMut_ );
    if ( commands_.empty() )
        return false;

    const PairTypeC &c = commands_.front();
    command = c.first;
    hostIP = c.second;
    commands_.pop();
    return true;
}

void WorkerManager::SetWorkerIP( WorkerPtr &worker, const std::string &ip )
{
    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    GrpNameToWorkerList::iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        WorkerList &workerList = it->second;
        workerList.SetWorkerIP( worker, ip );
    }
}

bool WorkerManager::GetWorkerByIP( const std::string &ip, WorkerPtr &worker ) const
{
    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    GrpNameToWorkerList::const_iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        WorkerList &workerList = it->second;
        if ( workerList.GetWorkerByIP( ip, worker ) )
            return true;

    }
    return false;
}

int WorkerManager::GetTotalWorkers() const
{
    int total = 0;

    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    GrpNameToWorkerList::const_iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        const WorkerList &workerList = it->second;
        total += workerList.GetTotalWorkers();
    }
    return total;
}

int WorkerManager::GetTotalCPU() const
{
    int total = 0;

    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    GrpNameToWorkerList::const_iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        const WorkerList &workerList = it->second;
        total += workerList.GetTotalCPU();
    }
    return total;
}

void WorkerManager::Shutdown()
{
    boost::mutex::scoped_lock scoped_lock( workersMut_ );

    GrpNameToWorkerList::iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        WorkerList &workerList = it->second;
        workerList.Clear();
    }
}

bool ReadHosts( const char *filePath, std::list< std::string > &hosts )
{
    int numHosts = 0;
    std::ifstream file( filePath );
    if ( !file.is_open() )
    {
        PS_LOG( "ReadHosts: couldn't open " << filePath );
        return false;
    }
    try
    {
        while( file.good() )
        {
            std::string host;
            getline( file, host );
            if ( isdigit( host[0] ) )
            {
                boost::system::error_code error;
                boost::asio::ip::address::from_string( host.c_str(), error );
                if ( error )
                {
                    PS_LOG( "invalid host ip: " << host );
                    continue;
                }
            }
            hosts.push_back( host );
            ++numHosts;
        }
    }
    catch( std::exception &e )
    {
        PS_LOG( "ReadHosts: failed " << e.what() );
        return false;
    }
    PS_LOG( numHosts << " hosts are readed" );
    return true;
}

} // namespace master

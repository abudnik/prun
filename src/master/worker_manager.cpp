#include <fstream>
#include <boost/asio/ip/address.hpp>
#include "worker_manager.h"
#include "common/log.h"
#include "sheduler.h"

namespace master {

void WorkerManager::CheckDropedPingResponses()
{
    std::vector< Worker * > changedWorkers;

    WorkerList::WorkerContainer &workers = workers_.GetWorkers();
    WorkerList::WorkerContainer::const_iterator it = workers.begin();
    for( ; it != workers.end(); ++it )
    {
        Worker *worker = *it;
        WorkerState state = worker->GetState();
        if ( !worker->GetNumPingResponse() )
        {
            if ( state == WORKER_STATE_READY )
            {
                worker->SetState( WORKER_STATE_NOT_AVAIL );
                changedWorkers.push_back( worker );
                PS_LOG( "node not available, ip= " << worker->GetIP() );
            }
            if ( state == WORKER_STATE_EXEC )
            {
                worker->SetState( WORKER_STATE_NOT_AVAIL );
                changedWorkers.push_back( worker );
                PS_LOG( "node job isn't available, ip= " << worker->GetIP() );
            }
        }
        worker->SetNumPingResponse( 0 );
    }

    if ( changedWorkers.size() )
    {
        Sheduler::Instance().OnChangedWorkerState( changedWorkers );
    }
}

void WorkerManager::OnNodePingResponse( const std::string &hostIP )
{
    Worker *worker = GetWorkerByIP( hostIP );
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
            Sheduler::Instance().OnHostAppearance( worker );
        }
    }
    else
    {
        PS_LOG( "WorkerManager::OnHostPingResponse worker not found, ip= " << hostIP );
    }
}

void WorkerManager::OnNodeJobCompletion( const std::string &hostIP, int64_t jobId, int taskId )
{
}

void WorkerManager::SetWorkerIP( Worker *worker, const std::string &ip )
{
    workers_.SetWorkerIP( worker, ip );
}

Worker *WorkerManager::GetWorkerByIP( const std::string &ip ) const
{
    return workers_.GetWorkerByIP( ip );
}

void WorkerManager::Shutdown()
{
	workers_.Clear();
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

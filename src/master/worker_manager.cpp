#include <fstream>
#include <boost/asio/ip/address.hpp>
#include <cctype>
#include "worker_manager.h"
#include "common/log.h"

namespace master {

void WorkerManager::CheckDropedPingResponses()
{
    bool stateChanged = false;
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
                stateChanged = true;
            }
            if ( state == WORKER_STATE_EXEC )
            {
                worker->SetState( WORKER_STATE_FAILED );
                stateChanged = true;
            }
        }
        worker->SetNumPingResponse( 0 );
    }

    //if ( stateChanged ) need_reshed();
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

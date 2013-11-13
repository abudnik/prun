#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <stdint.h> // boost/atomic/atomic.hpp:202:16: error: ‘uintptr_t’ was not declared in this scope
#include "ping.h"
#include "common/log.h"
#include "worker_manager.h"

namespace master {

void Pinger::Stop()
{
    stopped_ = true;
    timer_.StopWaiting();
}

void Pinger::PingWorkers()
{
    WorkerList::WorkerContainer &workers = WorkerManager::Instance().GetWorkers();
    WorkerList::WorkerContainer::iterator it = workers.begin();
    for( ; it != workers.end(); ++it )
    {
        PingWorker( *it );
    }
    ++numPings_;
}

void Pinger::Run()
{
    while( !stopped_ )
    {
        timer_.Wait( pingTimeout_ * 1000 );
        PingWorkers();
        CheckDropedPingResponses();
    }
}

void Pinger::CheckDropedPingResponses()
{
    if ( numPings_ < maxDroped_ + 1 )
        return;

    WorkerManager::Instance().CheckDropedPingResponses();
    numPings_ = 0;
}

void Pinger::OnWorkerIPResolve( Worker *worker, const std::string &ip )
{
    WorkerManager::Instance().SetWorkerIP( worker, ip );
}

void PingerBoost::StartPing()
{
    io_service_.post( boost::bind( &Pinger::Run, this ) );
}

void PingerBoost::PingWorker( Worker *worker )
{
    EndpointMap::iterator it = endpoints_.find( worker->GetHost() );
    if ( it == endpoints_.end() )
    {
        udp::resolver::query query( udp::v4(), worker->GetHost(), port_ );

        boost::system::error_code error;
        udp::resolver::iterator iterator = resolver_.resolve( query, error );
        if ( error )
        {
            PS_LOG( "PingerBoost::PingWorker address not resolved: " << worker->GetHost() );
            return;
        }

        std::pair< EndpointMap::iterator, bool > p = endpoints_.insert(
            std::make_pair( worker->GetHost(), *iterator ) );
        it = p.first;

        OnWorkerIPResolve( worker, it->second.address().to_string() );
    }

    const std::string &node_ip = it->second.address().to_string();

    std::string msg;
    protocol_->NodePing( msg, node_ip );
    //PS_LOG( msg );
    //PS_LOG( node_ip );

    udp::socket socket( io_service_, udp::endpoint( it->second.protocol(), 0 ) );
    try
    {
        socket.send_to( boost::asio::buffer( msg ), it->second );
    }
    catch( boost::system::system_error &e )
    {
        PS_LOG( "PingerBoost::PingWorker: send_to failed: " << e.what() << ", host : " << node_ip );
    }
}

} // namespace master

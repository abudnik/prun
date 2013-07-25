#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include "ping.h"
#include "common/log.h"

namespace master {

void Pinger::Stop()
{
    timer_.StopWaiting();
}

void Pinger::PingWorkers()
{
    WorkerList::WorkerContainer &workers = workerMgr_.GetWorkers();
    WorkerList::WorkerContainer::iterator it = workers.begin();
    for( ; it != workers.end(); ++it )
    {
        PingWorker( *it );
    }
    ++numPings_;
}

void Pinger::Run()
{
    do
    {
        PingWorkers();
        CheckDropedPingResponses();
    }
    while( timer_.Wait( pingTimeout_ * 1000 ) );
}

void Pinger::CheckDropedPingResponses()
{
    if ( numPings_ < maxDroped_ + 1 )
        return;

    workerMgr_.CheckDropedPingResponses();
    numPings_ = 0;
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
    }

    const std::string &node_ip = it->second.address().to_string();

	std::string msg;
	protocol_->NodePing( msg, node_ip );
	PS_LOG( msg );
    PS_LOG( node_ip );

    try
    {
        socket_.send_to( boost::asio::buffer( msg ), it->second );
    }
    catch( boost::system::system_error &e )
    {
        PS_LOG( "PingerBoost::PingWorker: send_to failed, host : " << node_ip );
    }
}

} // namespace master

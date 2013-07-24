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
}

void PingerBoost::StartPing()
{
    io_service_.post( boost::bind( &PingerBoost::Run, this ) );
}

void PingerBoost::Run()
{
    do
    {
        PingWorkers();
    }
    while( timer_.Wait( pingTimeout_ * 1000 ) );
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

	std::string msg;
	protocol_->NodePing( msg, GetHostIP() );
	PS_LOG( msg );
    PS_LOG( it->second );
	PS_LOG( socket_.send_to( boost::asio::buffer( msg ), it->second ) );
}

} // namespace master

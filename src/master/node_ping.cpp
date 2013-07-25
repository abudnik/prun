#include <boost/bind.hpp>
#include "node_ping.h"
#include "common/log.h"

namespace master {

void PingReceiver::OnNodePing( const std::string &node, const std::string &msg )
{
	PS_LOG( node << " : " << msg );
}

void PingReceiverBoost::Start()
{
    StartReceive();
}

void PingReceiverBoost::StartReceive()
{
    socket_.async_receive_from(
        boost::asio::buffer( buffer_ ), remote_endpoint_,
        boost::bind( &PingReceiverBoost::HandleRead, this,
                     boost::asio::placeholders::error,
                     boost::asio::placeholders::bytes_transferred ) );
}

void PingReceiverBoost::HandleRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( !error )
    {
		std::string response( buffer_.begin(), buffer_.end() );
        OnNodePing( remote_endpoint_.address().to_string(), response );
    }
    else
    {
        PS_LOG( "PingReceiverBoost::HandleRead error=" << error );
    }

    StartReceive();
}

} // namespace master


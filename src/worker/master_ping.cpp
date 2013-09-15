#include <boost/bind.hpp>
#include "master_ping.h"
#include "common/log.h"

namespace python_server {

void MasterPingBoost::Start()
{
    StartReceive();
}

void MasterPingBoost::StartReceive()
{
    socket_.async_receive_from(
        boost::asio::buffer( buffer_ ), remote_endpoint_,
        boost::bind( &MasterPingBoost::HandleRead, this,
                     boost::asio::placeholders::error,
                     boost::asio::placeholders::bytes_transferred ) );
}

void MasterPingBoost::HandleRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( !error )
    {
        //std::string request( buffer_.begin(), buffer_.begin() + bytes_transferred );
        //PS_LOG( request );

        std::string msg;
        protocol_->NodeResponsePing( msg );

        udp::endpoint master_endpoint( remote_endpoint_.address(), DEFAULT_MASTER_UDP_PORT );

        try
        {
            socket_.send_to( boost::asio::buffer( msg ), master_endpoint );
        }
        catch( boost::system::system_error &e )
        {
            PS_LOG( "MasterPingBoost::HandleRead: send_to failed: " << e.what() << ", host : " << remote_endpoint_ );
        }
    }
    else
    {
        PS_LOG( "MasterPingBoost::HandleRead error=" << error );
    }

    StartReceive();
}

} // namespace python_server


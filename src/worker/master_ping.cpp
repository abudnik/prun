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
        std::stringstream ss;
        ss << buffer_.c_array();
        PS_LOG( ss.str() );

        std::string msg;
        protocol_->NodeResponsePing( msg );

        try
        {
            socket_.send_to( boost::asio::buffer( msg ), remote_endpoint_ );
        }
        catch( boost::system::system_error &e )
        {
            PS_LOG( "MasterPingBoost::HandleRead: send_to failed, host : " << remote_endpoint_ );
        }
    }
    else
    {
        PS_LOG( "MasterPingBoost::HandleRead error=" << error );
    }

    StartReceive();
}

} // namespace python_server


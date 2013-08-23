#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include "admin.h"

namespace master {

void AdminSession::Start()
{
    //boost::asio::ip::address remoteAddress = socket_.remote_endpoint().address();

    socket_.async_read_some( boost::asio::buffer( buffer_ ),
                             boost::bind( &AdminSession::FirstRead, shared_from_this(),
                                          boost::asio::placeholders::error,
                                          boost::asio::placeholders::bytes_transferred ) );
}

void AdminSession::FirstRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    /*if ( !error )
    {
        int ret = request_.OnFirstRead( buffer_, bytes_transferred );
        if ( ret == 0 )
        {
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &Session::FirstRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
            return;
        }
        if ( ret < 0 )
        {
            OnReadCompletion( false );
            return;
        }
    }
    else
    {
        PS_LOG( "Session::FirstRead error=" << error.message() );
    }

    HandleRead( error, bytes_transferred );*/
}

void AdminConnection::StartAccept()
{
    session_ptr session( new AdminSession( io_service_ ) );
    acceptor_.async_accept( session->GetSocket(),
                            boost::bind( &AdminConnection::HandleAccept, this,
                                        session, boost::asio::placeholders::error ) );
}

void AdminConnection::HandleAccept( session_ptr session, const boost::system::error_code &error )
{
    if ( !error )
    {
        PS_LOG( "admin connection accepted..." );
        io_service_.post( boost::bind( &AdminSession::Start, session ) );
        StartAccept();
    }
    else
    {
        PS_LOG( "AdminConnection::HandleAccept: " << error.message() );
    }
}

} // namespace master

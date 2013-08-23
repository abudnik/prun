#ifndef __ADMIN_H
#define __ADMIN_H

#include <boost/asio.hpp>
#include "common/request.h"
#include "common/log.h"
#include "defines.h"

namespace master {

using boost::asio::ip::tcp;

class AdminSession : public boost::enable_shared_from_this< AdminSession >
{
	typedef boost::array< char, 32 * 1024 > BufferType;

public:
	AdminSession( boost::asio::io_service &io_service )
	: socket_( io_service ),
     request_( false ),
	 io_service_( io_service )
	{}

    void Start();

	tcp::socket &GetSocket() { return socket_; }

private:
    void FirstRead( const boost::system::error_code& error, size_t bytes_transferred );

private:
	tcp::socket socket_;
	BufferType buffer_;
    python_server::Request< BufferType > request_;
	boost::asio::io_service &io_service_;
};

class AdminConnection
{
	typedef boost::shared_ptr< AdminSession > session_ptr;

public:
    AdminConnection( boost::asio::io_service &io_service )
    : io_service_( io_service ),
     acceptor_( io_service )
    {
		try
		{
		    tcp::endpoint endpoint( tcp::v4(), MASTER_ADMIN_PORT );
			acceptor_.open( endpoint.protocol() );
			acceptor_.set_option( tcp::acceptor::reuse_address( true ) );
			acceptor_.set_option( tcp::no_delay( true ) );
			acceptor_.bind( endpoint );
			acceptor_.listen();
		}
		catch( std::exception &e )
		{
			PS_LOG( "AdminConnection: " << e.what() );
		}

		StartAccept();
    }

private:
    void StartAccept();
    void HandleAccept( session_ptr session, const boost::system::error_code &error );

private:
	boost::asio::io_service &io_service_;
	tcp::acceptor acceptor_;
};

} // namespace master

#endif

#ifndef __ADMIN_H
#define __ADMIN_H

#include <boost/asio.hpp>
#include <boost/property_tree/ptree.hpp>
#include "common/request.h"
#include "common/log.h"
#include "defines.h"

namespace master {

using boost::asio::ip::tcp;

class AdminSession;

class AdminCommand
{
public:
    virtual ~AdminCommand() {}
    virtual void Execute( const std::string &command,
                          const boost::property_tree::ptree &ptree,
                          AdminSession *session ) = 0;
};

class Job;
class AdminCommand_Run : public AdminCommand
{
public:
    virtual void Execute( const std::string &command,
                          const boost::property_tree::ptree &ptree,
                          AdminSession *session );

private:
    void PrintJobInfo( Job *job, AdminSession *session ) const;
};

class AdminCommand_Stop : public AdminCommand
{
public:
    virtual void Execute( const std::string &command,
                          const boost::property_tree::ptree &ptree,
                          AdminSession *session );
};

class AdminCommand_Info : public AdminCommand
{
public:
    virtual void Execute( const std::string &command,
                          const boost::property_tree::ptree &ptree,
                          AdminSession *session );
};

class AdminCommand_Stat : public AdminCommand
{
public:
    virtual void Execute( const std::string &command,
                          const boost::property_tree::ptree &ptree,
                          AdminSession *session );
};

class AdminCommandDispatcher
{
public:
    void Initialize();
    void Shutdown();
    AdminCommand *Get( const std::string &command ) const;

    static AdminCommandDispatcher &Instance()
    {
        static AdminCommandDispatcher instance_;
        return instance_;
    }

private:
    std::map< std::string, AdminCommand * > map_;
};

class AdminSession : public boost::enable_shared_from_this< AdminSession >
{
    typedef boost::array< char, 32 * 1024 > BufferType;

public:
    AdminSession( boost::asio::io_service &io_service )
    : socket_( io_service ),
     request_( true ),
     io_service_( io_service )
    {}

    ~AdminSession()
    {
        if ( !remoteIP_.empty() )
            PS_LOG( "~AdminSession " << remoteIP_ );
    }

    void Start();

    tcp::socket &GetSocket() { return socket_; }

    void OnCommandCompletion( const std::string &result );

private:
    void FirstRead( const boost::system::error_code &error, size_t bytes_transferred );
    void HandleRead( const boost::system::error_code &error, size_t bytes_transferred );
    void HandleWrite( const boost::system::error_code& error, size_t bytes_transferred );

    void HandleRequest();

private:
    tcp::socket socket_;
    BufferType buffer_;
    common::Request< BufferType > request_;
    std::string remoteIP_;
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

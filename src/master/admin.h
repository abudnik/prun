#ifndef __ADMIN_H
#define __ADMIN_H

#include <boost/asio.hpp>
#include <boost/property_tree/ptree.hpp>
#include "common/json_rpc.h"
#include "common/config.h"
#include "common/log.h"
#include "defines.h"

namespace master {

using boost::asio::ip::tcp;


class Job;
class AdminCommand_Run : public common::JsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );

private:
    int RunJob( std::ifstream &file, std::string &result ) const;
    int RunMetaJob( std::ifstream &file, std::string &result ) const;
    void PrintJobInfo( const Job *job, std::string &result ) const;
};

class AdminCommand_Stop : public common::JsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_StopGroup : public common::JsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_Info : public common::JsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_Stat : public common::JsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};


class AdminSession : public boost::enable_shared_from_this< AdminSession >
{
    typedef boost::array< char, 32 * 1024 > BufferType;

public:
    AdminSession( boost::asio::io_service &io_service )
    : socket_( io_service ),
     io_service_( io_service )
    {}

    ~AdminSession()
    {
        if ( !remoteIP_.empty() )
            PS_LOG( "~AdminSession " << remoteIP_ );
    }

    static void InitializeRpcHandlers();

    void Start();

    tcp::socket &GetSocket() { return socket_; }

private:
    void HandleRead( const boost::system::error_code &error, size_t bytes_transferred );
    void HandleWrite( const boost::system::error_code& error, size_t bytes_transferred );

    void HandleRequest();

private:
    tcp::socket socket_;
    BufferType buffer_;
    std::string request_, response_;
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
            common::Config &cfg = common::Config::Instance();
            bool ipv6 = cfg.Get<bool>( "ipv6" );

            tcp::endpoint endpoint( ipv6 ? tcp::v6() : tcp::v4(), MASTER_ADMIN_PORT );
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

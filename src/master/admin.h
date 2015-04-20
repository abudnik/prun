/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2013 Andrey Budnik <budnik27@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

===========================================================================
*/

#ifndef __ADMIN_H
#define __ADMIN_H

#include <boost/asio.hpp>
#include <boost/property_tree/ptree.hpp>
#include "common/json_rpc.h"
#include "common/config.h"
#include "common/log.h"

namespace master {

using boost::asio::ip::tcp;

class AdminCommand_Run : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_Stop : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_StopGroup : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_StopAll : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_StopPrevious : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_AddHosts : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_DeleteHosts : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_AddGroup : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_DeleteGroup : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_Info : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_Stat : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_Jobs : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};

class AdminCommand_Ls : public common::IJsonRpcHandler
{
public:
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result );
};


class AdminSession : public boost::enable_shared_from_this< AdminSession >
{
    typedef boost::array< char, 32 * 1024 > BufferType;

public:
    AdminSession( boost::asio::io_service &io_service,
                  common::JsonRpc &requestHandler )
    : socket_( io_service ),
     requestHandler_( requestHandler )
    {}

    ~AdminSession()
    {
        if ( !remoteIP_.empty() )
            PLOG( "~AdminSession " << remoteIP_ );
    }

    void Start();

    tcp::socket &GetSocket() { return socket_; }

private:
    void HandleRead( const boost::system::error_code &error, size_t bytes_transferred );
    void HandleWrite( const boost::system::error_code& error, size_t bytes_transferred );

    void HandleRequest();

private:
    tcp::socket socket_;
    common::JsonRpc &requestHandler_;
    BufferType buffer_;
    std::string request_, response_;
    std::string remoteIP_;
};

class AdminConnection
{
    typedef boost::shared_ptr< AdminSession > session_ptr;

public:
    AdminConnection( boost::asio::io_service &io_service )
    : io_service_( io_service ),
     acceptor_( io_service )
    {
        InitializeRpcHandlers();

        try
        {
            const common::Config &cfg = common::Config::Instance();
            const bool ipv6 = cfg.Get<bool>( "ipv6" );
            const unsigned short master_admin_port = cfg.Get<unsigned short>( "master_admin_port" );

            tcp::endpoint endpoint( ipv6 ? tcp::v6() : tcp::v4(), master_admin_port );
            acceptor_.open( endpoint.protocol() );
            acceptor_.set_option( tcp::acceptor::reuse_address( true ) );
            acceptor_.set_option( tcp::no_delay( true ) );
            acceptor_.bind( endpoint );
            acceptor_.listen();
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "AdminConnection: " << e.what() );
        }

        StartAccept();
    }

private:
    void InitializeRpcHandlers();

    void StartAccept();
    void HandleAccept( session_ptr session, const boost::system::error_code &error );

private:
    boost::asio::io_service &io_service_;
    tcp::acceptor acceptor_;

    common::JsonRpc requestHandler_;
};

} // namespace master

#endif

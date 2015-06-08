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

#define BOOST_SPIRIT_THREADSAFE

#include <sstream>
#include <boost/property_tree/json_parser.hpp>
#include <boost/filesystem.hpp>
#include <boost/bind.hpp>
#include "admin.h"
#include "user_command.h"
#include "job_manager.h"
#include "worker_manager.h"
#include "common/crutches.h"
#include "common/service_locator.h"

namespace master {

int AdminCommand_Run::Execute( const boost::property_tree::ptree &params,
                               std::string &result )
{
    std::string filePath, jobAlias;
    try
    {
        filePath = params.get<std::string>( "file" );
        if ( filePath[0] != '/' )
        {
            IJobManager *jobManager = common::GetService< IJobManager >();
            filePath = jobManager->GetJobsDir() + '/' + filePath;
        }

        if ( params.count( "alias" ) > 0 )
        {
            jobAlias = params.get<std::string>( "alias" );
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Run::Execute: " << e.what() );
        return JSON_RPC_INVALID_PARAMS;
    }

    if ( !UserCommand().Run( filePath, jobAlias, result ) )
        return JSON_RPC_INTERNAL_ERROR;

    return 0;
}

int AdminCommand_Stop::Execute( const boost::property_tree::ptree &params,
                                std::string &result )
{
    std::string jobName;
    int64_t jobId = -1;
    try
    {
        std::string value = params.get<std::string>( "job_id" );
        if ( !value.empty() && isalpha( value[0] ) )
        {
            jobName = value;
        }
        else
        {
            jobId = params.get<int64_t>( "job_id" );
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Stop::Execute: " << e.what() );
        return JSON_RPC_INVALID_PARAMS;
    }

    if ( jobName.empty() )
    {
        if ( !UserCommand().Stop( jobId ) )
            return JSON_RPC_INTERNAL_ERROR;
    }
    else
    {
        if ( !UserCommand().StopNamed( jobName ) )
            return JSON_RPC_INTERNAL_ERROR;
    }

    return 0;
}

int AdminCommand_StopGroup::Execute( const boost::property_tree::ptree &params,
                                     std::string &result )
{
    int64_t groupId;
    try
    {
        groupId = params.get<int64_t>( "group_id" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_StopGroup::Execute: " << e.what() );
        return JSON_RPC_INVALID_PARAMS;
    }

    if ( !UserCommand().StopGroup( groupId ) )
        return JSON_RPC_INTERNAL_ERROR;

    return 0;
}

int AdminCommand_StopAll::Execute( const boost::property_tree::ptree &params,
                                   std::string &result )
{
    if ( !UserCommand().StopAll() )
        return JSON_RPC_INTERNAL_ERROR;

    return 0;
}

int AdminCommand_StopPrevious::Execute( const boost::property_tree::ptree &params,
                                        std::string &result )
{
    if ( !UserCommand().StopPreviousJobs() )
        return JSON_RPC_INTERNAL_ERROR;

    return 0;
}

int AdminCommand_AddHosts::Execute( const boost::property_tree::ptree &params,
                                    std::string &result )
{
    try
    {
        int i = 0;
        std::string groupName, host;
        for( const auto &v : params.get_child( "hosts" ) )
        {
            if ( i++ % 2 > 0 )
            {
                groupName = v.second.get_value< std::string >();
                UserCommand().AddWorkerHost( groupName, host );
            }
            else
                host = v.second.get_value< std::string >();
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_AddHosts::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}

int AdminCommand_DeleteHosts::Execute( const boost::property_tree::ptree &params,
                                       std::string &result )
{
    try
    {;
        std::string host;
        for( const auto &v : params.get_child( "hosts" ) )
        {
            host = v.second.get_value< std::string >();
            UserCommand().DeleteHost( host );
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_DeleteHosts::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}


int AdminCommand_AddGroup::Execute( const boost::property_tree::ptree &params,
                                    std::string &result )
{
    std::string filePath, fileName;
    try
    {
        filePath = params.get<std::string>( "file" );
        if ( filePath[0] != '/' )
        {
            IWorkerManager *workerManager = common::GetService< IWorkerManager >();
            filePath = workerManager->GetConfigDir() + '/' + filePath;
        }

        boost::filesystem::path p( filePath );
        fileName = common::PathToString( p.filename() );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_AddGroup::Execute: " << e.what() );
        return JSON_RPC_INVALID_PARAMS;
    }

    if ( !UserCommand().AddGroup( filePath, fileName ) )
        return JSON_RPC_INTERNAL_ERROR;

    return 0;
}

int AdminCommand_DeleteGroup::Execute( const boost::property_tree::ptree &params,
                                       std::string &result )
{
    std::string group;
    try
    {
        group = params.get<std::string>( "group" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_DeleteGroup::Execute: " << e.what() );
        return JSON_RPC_INVALID_PARAMS;
    }

    if ( !UserCommand().DeleteGroup( group ) )
        return JSON_RPC_INTERNAL_ERROR;

    return 0;
}

int AdminCommand_Info::Execute( const boost::property_tree::ptree &params,
                                std::string &result )
{
    int64_t jobId;
    try
    {
        jobId = params.get<int64_t>( "job_id" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Info::Execute: " << e.what() );
        return JSON_RPC_INVALID_PARAMS;
    }

    if ( !UserCommand().Info( jobId, result ) )
        return JSON_RPC_INTERNAL_ERROR;

    return 0;
}


int AdminCommand_Stat::Execute( const boost::property_tree::ptree &params,
                                std::string &result )
{
    if ( !UserCommand().GetStatistics( result ) )
        return JSON_RPC_INTERNAL_ERROR;

    return 0;
}

int AdminCommand_Jobs::Execute( const boost::property_tree::ptree &params,
                                std::string &result )
{
    if ( !UserCommand().GetAllJobInfo( result ) )
        return JSON_RPC_INTERNAL_ERROR;

    return 0;
}

int AdminCommand_Ls::Execute( const boost::property_tree::ptree &params,
                              std::string &result )
{
    if ( !UserCommand().GetWorkersStatistics( result ) )
        return JSON_RPC_INTERNAL_ERROR;

    return 0;
}

int AdminCommand_Cron::Execute( const boost::property_tree::ptree &params,
                                std::string &result )
{
    if ( !UserCommand().GetCronInfo( result ) )
        return JSON_RPC_INTERNAL_ERROR;

    return 0;
}


void AdminSession::Start()
{
    remoteIP_ = socket_.remote_endpoint().address().to_string();

    PLOG( "AdminSession::Start: ip=" << remoteIP_ );

    socket_.async_read_some( boost::asio::buffer( buffer_ ),
                             boost::bind( &AdminSession::HandleRead, shared_from_this(),
                                          boost::asio::placeholders::error,
                                          boost::asio::placeholders::bytes_transferred ) );
}

void AdminSession::HandleRead( const boost::system::error_code &error, size_t bytes_transferred )
{
    if ( !error )
    {
        request_.append( buffer_.begin(), buffer_.begin() + bytes_transferred );

        if ( !common::JsonRpc::ValidateJsonBraces( request_ ) )
        {
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &AdminSession::HandleRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
        }
        else
        {
            HandleRequest();

            // read next command
            request_.clear();
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &AdminSession::HandleRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
        }
    }
    else
    {
        PLOG_WRN( "AdminSession::HandleRead error=" << error.message() );
    }
}

void AdminSession::HandleWrite( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( error )
    {
        PLOG_ERR( "AdminSession::HandleWrite error=" << error.message() );
    }
}

void AdminSession::HandleRequest()
{
    PLOG( request_ );
    std::string requestId, result;
    int errCode = requestHandler_.HandleRequest( request_, requestId, result );

    try
    {
        boost::property_tree::ptree ptree;
        ptree.put( "jsonrpc", common::JsonRpc::GetProtocolVersion() );
        ptree.put( "id", requestId );

        if ( errCode )
        {
            std::string description;
            requestHandler_.GetErrorDescription( errCode, description );
            boost::property_tree::ptree perror;
            perror.put( "code", errCode );
            perror.put( "message", description );
            ptree.add_child( "error", perror );
        }
        else
        {
            ptree.put( "result", result );
        }

        std::ostringstream ss;
        boost::property_tree::write_json( ss, ptree, false );
        response_ = ss.str();

        boost::asio::async_write( socket_,
                                  boost::asio::buffer( response_ ),
                                  boost::bind( &AdminSession::HandleWrite, shared_from_this(),
                                               boost::asio::placeholders::error,
                                               boost::asio::placeholders::bytes_transferred ) );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminSession::HandleRequest: " << e.what() );
    }
}


void AdminConnection::InitializeRpcHandlers()
{
    common::JsonRpc &rpc = requestHandler_;
    rpc.RegisterHandler( "run",          new AdminCommand_Run );
    rpc.RegisterHandler( "stop",         new AdminCommand_Stop );
    rpc.RegisterHandler( "stop_group",   new AdminCommand_StopGroup );
    rpc.RegisterHandler( "stop_all",     new AdminCommand_StopAll );
    rpc.RegisterHandler( "stop_prev",    new AdminCommand_StopPrevious );
    rpc.RegisterHandler( "add_hosts",    new AdminCommand_AddHosts );
    rpc.RegisterHandler( "delete_hosts", new AdminCommand_DeleteHosts );
    rpc.RegisterHandler( "add_group",    new AdminCommand_AddGroup );
    rpc.RegisterHandler( "delete_group", new AdminCommand_DeleteGroup );
    rpc.RegisterHandler( "info",         new AdminCommand_Info );
    rpc.RegisterHandler( "stat",         new AdminCommand_Stat );
    rpc.RegisterHandler( "jobs",         new AdminCommand_Jobs );
    rpc.RegisterHandler( "ls",           new AdminCommand_Ls );
    rpc.RegisterHandler( "cron",         new AdminCommand_Cron );
}

void AdminConnection::StartAccept()
{
    session_ptr session( new AdminSession( io_service_, requestHandler_ ) );
    acceptor_.async_accept( session->GetSocket(),
                            boost::bind( &AdminConnection::HandleAccept, this,
                                         session, boost::asio::placeholders::error ) );
}

void AdminConnection::HandleAccept( session_ptr session, const boost::system::error_code &error )
{
    if ( !error )
    {
        io_service_.post( boost::bind( &AdminSession::Start, session ) );
        StartAccept();
    }
    else
    {
        PLOG_ERR( "AdminConnection::HandleAccept: " << error.message() );
    }
}

} // namespace master

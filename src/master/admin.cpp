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
#include <boost/foreach.hpp>
#include <boost/filesystem.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include "admin.h"
#include "job_manager.h"
#include "worker_manager.h"
#include "scheduler.h"
#include "common/crutches.h"

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
            filePath = JobManager::Instance().GetJobsDir() + '/' + filePath;
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

    try
    {
        // read job description from file
        std::ifstream file( filePath.c_str() );
        if ( !file.is_open() )
        {
            PLOG_ERR( "AdminCommand_Run::Execute: couldn't open '" << filePath << "'" );
            return JSON_RPC_INTERNAL_ERROR;
        }

        size_t found = filePath.rfind( '.' );
        if ( found == std::string::npos )
        {
            PLOG_ERR( "AdminCommand_Run::Execute: couldn't extract job file extension '" << filePath << "'" );
            return JSON_RPC_INTERNAL_ERROR;
        }
        std::string ext = filePath.substr( found + 1 );

        if ( ext == "job" )
            return RunJob( file, jobAlias, result );
        else
        if ( ext == "meta" )
            return RunMetaJob( file, result );
        else
            PLOG_ERR( "AdminCommand_Run::Execute: unknown file extension '" << ext << "'" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Run::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}

int AdminCommand_Run::RunJob( std::ifstream &file, const std::string &jobAlias, std::string &result ) const
{
    try
    {
        std::string jobDescr, line;
        while( std::getline( file, line ) )
            jobDescr += line;

        Job *job = JobManager::Instance().CreateJob( jobDescr );
        if ( job )
        {
            job->SetAlias( jobAlias );
            PrintJobInfo( job, result );
            // add job to job queue
            JobManager::Instance().PushJob( job );
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Run::RunJob: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}

int AdminCommand_Run::RunMetaJob( std::ifstream &file, std::string &result ) const
{
    try
    {
        std::string metaDescr, line;
        while( getline( file, line ) )
            metaDescr += line + '\n';

        std::list< JobPtr > jobs;
        JobManager::Instance().CreateMetaJob( metaDescr, jobs );
        JobManager::Instance().PushJobs( jobs );

        std::ostringstream ss;
        ss << "----------------" << std::endl;
        std::list< JobPtr >::const_iterator it = jobs.begin();
        for( ; it != jobs.end(); ++it )
        {
            PrintJobInfo( (*it).get(), result );
            ss << result << std::endl;
        }
        ss << "----------------" << std::endl;
        result = ss.str();
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Run::RunMetaJob: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}

void AdminCommand_Run::PrintJobInfo( const Job *job, std::string &result ) const
{
    std::ostringstream ss;
    ss << "jobId = " << job->GetJobId() << ", groupId = " << job->GetGroupId();
    result = ss.str();
}

int AdminCommand_Stop::Execute( const boost::property_tree::ptree &params,
                                std::string &result )
{
    int64_t jobId;
    try
    {
        jobId = params.get<int64_t>( "job_id" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Stop::Execute: " << e.what() );
        return JSON_RPC_INVALID_PARAMS;
    }

    try
    {
        if ( !JobManager::Instance().DeleteJob( jobId ) )
        {
            Scheduler::Instance().StopJob( jobId );
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Stop::Execute: " << e.what() );
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

    try
    {
        JobManager::Instance().DeleteJobGroup( groupId );
        Scheduler::Instance().StopJobGroup( groupId );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_StopGroup::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}

int AdminCommand_StopAll::Execute( const boost::property_tree::ptree &params,
                                   std::string &result )
{
    try
    {
        JobManager::Instance().DeleteAllJobs();
        Scheduler::Instance().StopAllJobs();
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_StopAll::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}

int AdminCommand_StopPrevious::Execute( const boost::property_tree::ptree &params,
                                        std::string &result )
{
    try
    {
        Scheduler::Instance().StopPreviousJobs();
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_StopPrevious::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}

int AdminCommand_AddHosts::Execute( const boost::property_tree::ptree &params,
                                    std::string &result )
{
    try
    {
        WorkerManager &mgr = WorkerManager::Instance();
        int i = 0;
        std::string groupName, host;
        BOOST_FOREACH( const boost::property_tree::ptree::value_type &v,
                       params.get_child( "hosts" ) )
        {
            if ( i++ % 2 > 0 )
            {
                groupName = v.second.get_value< std::string >();
                mgr.AddWorkerHost( groupName, host );
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
    {
        WorkerManager &mgr = WorkerManager::Instance();
        Scheduler &scheduler = Scheduler::Instance();
        std::string host;
        BOOST_FOREACH( const boost::property_tree::ptree::value_type &v,
                       params.get_child( "hosts" ) )
        {
            host = v.second.get_value< std::string >();
            mgr.DeleteWorkerHost( host );
            scheduler.DeleteWorker( host );
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

        boost::filesystem::path p( filePath );
        fileName = common::PathToString( p.filename() );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_AddGroup::Execute: " << e.what() );
        return JSON_RPC_INVALID_PARAMS;
    }

    try
    {
        std::list< std::string > hosts;
        if ( ReadHosts( filePath.c_str(), hosts ) )
        {
            WorkerManager::Instance().AddWorkerGroup( fileName, hosts );
        }
        else
        {
            return JSON_RPC_INTERNAL_ERROR;
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_AddGroup::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
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

    try
    {
        std::vector< WorkerPtr > workers;
        WorkerManager::Instance().GetWorkers( workers, group );
        WorkerManager::Instance().DeleteWorkerGroup( group );

        Scheduler &scheduler = Scheduler::Instance();
        std::vector< WorkerPtr >::const_iterator it = workers.begin();
        for( ; it != workers.end(); ++it )
        {
            const WorkerPtr &w = *it;
            scheduler.DeleteWorker( w->GetHost() );
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_DeleteGroup::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
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

    try
    {
        Scheduler::Instance().GetJobInfo( result, jobId );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Info::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}


int AdminCommand_Stat::Execute( const boost::property_tree::ptree &params,
                                std::string &result )
{
    try
    {
        Scheduler::Instance().GetStatistics( result );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Stat::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}

int AdminCommand_Jobs::Execute( const boost::property_tree::ptree &params,
                                std::string &result )
{
    try
    {
        Scheduler::Instance().GetAllJobInfo( result );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Jobs::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}

int AdminCommand_Ls::Execute( const boost::property_tree::ptree &params,
                              std::string &result )
{
    try
    {
        Scheduler::Instance().GetWorkersStatistics( result );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "AdminCommand_Ls::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}


void AdminSession::InitializeRpcHandlers()
{
    common::JsonRpc &rpc = common::JsonRpc::Instance();
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
}

void AdminSession::Start()
{
    remoteIP_ = socket_.remote_endpoint().address().to_string();

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
    int errCode = common::JsonRpc::Instance().HandleRequest( request_, requestId, result );

    try
    {
        boost::property_tree::ptree ptree;
        ptree.put( "jsonrpc", common::JsonRpc::GetProtocolVersion() );
        ptree.put( "id", requestId );

        if ( errCode )
        {
            std::string description;
            common::JsonRpc::Instance().GetErrorDescription( errCode, description );
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
        PLOG( "admin connection accepted..." );
        io_service_.post( boost::bind( &AdminSession::Start, session ) );
        StartAccept();
    }
    else
    {
        PLOG_ERR( "AdminConnection::HandleAccept: " << error.message() );
    }
}

} // namespace master

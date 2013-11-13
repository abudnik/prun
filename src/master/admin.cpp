#define BOOST_SPIRIT_THREADSAFE

#include <sstream>
#include <boost/property_tree/json_parser.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include "admin.h"
#include "job_manager.h"
#include "scheduler.h"

namespace master {

int AdminCommand_Run::Execute( const boost::property_tree::ptree &params,
                               std::string &result )
{
    std::string filePath;
    try
    {
        filePath = params.get<std::string>( "file" );
    }
    catch( std::exception &e )
    {
        PS_LOG( "AdminCommand_Run::Execute: " << e.what() );
        return JSON_RPC_INVALID_PARAMS;
    }

    try
    {
        // read job description from file
        std::ifstream file( filePath.c_str() );
        if ( !file.is_open() )
        {
            PS_LOG( "AdminCommand_Run::Execute: couldn't open " << filePath );
            return JSON_RPC_INTERNAL_ERROR;
        }
        std::string jobDescr, line;
        while( std::getline( file, line ) )
            jobDescr += line;

        Job *job = JobManager::Instance().CreateJob( jobDescr );
        if ( job )
        {
            PrintJobInfo( job, result );
            // job->SetCallback( session, &common::JsonRpcCaller::RpcCall );
            // add job to job queue
            JobManager::Instance().PushJob( job );
        }
    }
    catch( std::exception &e )
    {
        PS_LOG( "AdminCommand_Run::Execute: " << e.what() );
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
        PS_LOG( "AdminCommand_Stop::Execute: " << e.what() );
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
        PS_LOG( "AdminCommand_Stop::Execute: " << e.what() );
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
        PS_LOG( "AdminCommand_StopGroup::Execute: " << e.what() );
        return JSON_RPC_INVALID_PARAMS;
    }

    try
    {
        JobManager::Instance().DeleteJobGroup( groupId );
        Scheduler::Instance().StopJobGroup( groupId );
    }
    catch( std::exception &e )
    {
        PS_LOG( "AdminCommand_StopGroup::Execute: " << e.what() );
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
        PS_LOG( "AdminCommand_Info::Execute: " << e.what() );
        return JSON_RPC_INVALID_PARAMS;
    }

    try
    {
        Scheduler::Instance().GetJobInfo( result, jobId );
    }
    catch( std::exception &e )
    {
        PS_LOG( "AdminCommand_Info::Execute: " << e.what() );
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
        PS_LOG( "AdminCommand_Stat::Execute: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }
    return 0;
}


void AdminSession::InitializeRpcHandlers()
{
    common::JsonRpc &rpc = common::JsonRpc::Instance();
    rpc.RegisterHandler( "run",        new AdminCommand_Run );
    rpc.RegisterHandler( "stop",       new AdminCommand_Stop );
    rpc.RegisterHandler( "stop_group", new AdminCommand_StopGroup );
    rpc.RegisterHandler( "info",       new AdminCommand_Info );
    rpc.RegisterHandler( "stat",       new AdminCommand_Stat );
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
        PS_LOG( "AdminSession::HandleRead error=" << error.message() );
    }
}

void AdminSession::HandleWrite( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( error )
    {
        PS_LOG( "AdminSession::HandleWrite error=" << error.message() );
    }
}

void AdminSession::HandleRequest()
{
    PS_LOG( request_ );
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
        PS_LOG( "AdminSession::HandleRequest: " << e.what() );
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

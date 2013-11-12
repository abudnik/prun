#define BOOST_SPIRIT_THREADSAFE

#include <sstream>
#include <boost/property_tree/json_parser.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include "admin.h"
#include "job_manager.h"
#include "scheduler.h"

namespace master {

int AdminCommand_Run::Execute( const boost::property_tree::ptree &ptree,
                               common::JsonRpcCaller *caller )
{
    AdminSession *session = dynamic_cast< AdminSession * >( caller );
    try
    {
        std::string filePath = ptree.get<std::string>( "file" );
        // read job description from file
        std::ifstream file( filePath.c_str() );
        if ( !file.is_open() )
        {
            PS_LOG( "AdminCommand_Run::Execute: couldn't open " << filePath );
            return 0; // todo: error code
        }
        std::string jobDescr, line;
        while( std::getline( file, line ) )
            jobDescr += line;

        Job *job = JobManager::Instance().CreateJob( jobDescr );
        if ( job )
        {
            PrintJobInfo( job, caller );
            job->SetCallback( session, &common::JsonRpcCaller::RpcCall );
            // add job to job queue
            JobManager::Instance().PushJob( job );
        }
    }
    catch( std::exception &e )
    {
        PS_LOG( "AdminCommand_Run::Execute: " << e.what() );
    }
    return 0;
}

void AdminCommand_Run::PrintJobInfo( const Job *job, common::JsonRpcCaller *caller ) const
{
    std::ostringstream ss;
    ss << "Job pushed to queue, jobId = " << job->GetJobId() <<
        ", groupId = " << job->GetGroupId();

    boost::property_tree::ptree params;
    params.put( "job_id", job->GetJobId() );
    params.put( "group_id", job->GetGroupId() );
    params.put( "user_msg", ss.str() );

    caller->RpcCall( "on_command_run", params );
}

int AdminCommand_Stop::Execute( const boost::property_tree::ptree &ptree,
                                common::JsonRpcCaller *caller )
{
    try
    {
        int64_t jobId = ptree.get<int64_t>( "job_id" );
        if ( !JobManager::Instance().DeleteJob( jobId ) )
        {
            Scheduler::Instance().StopJob( jobId );
        }
    }
    catch( std::exception &e )
    {
        PS_LOG( "AdminCommand_Stop::Execute: " << e.what() );
    }
    return 0;
}

int AdminCommand_StopGroup::Execute( const boost::property_tree::ptree &ptree,
                                     common::JsonRpcCaller *caller )
{
    try
    {
        int64_t groupId = ptree.get<int64_t>( "group_id" );
        JobManager::Instance().DeleteJobGroup( groupId );
        Scheduler::Instance().StopJobGroup( groupId );
    }
    catch( std::exception &e )
    {
        PS_LOG( "AdminCommand_StopGroup::Execute: " << e.what() );
    }
    return 0;
}

int AdminCommand_Info::Execute( const boost::property_tree::ptree &ptree,
                                common::JsonRpcCaller *caller )
{
    try
    {
        int64_t jobId = ptree.get<int64_t>( "job_id" );
        std::string info;
        Scheduler::Instance().GetJobInfo( info, jobId );
        //session->OnCommandCompletion( info );
    }
    catch( std::exception &e )
    {
        PS_LOG( "AdminCommand_Info::Execute: " << e.what() );
    }
    return 0;
}


int AdminCommand_Stat::Execute( const boost::property_tree::ptree &ptree,
                                common::JsonRpcCaller *caller )
{
    try
    {
        std::string stat;
        Scheduler::Instance().GetStatistics( stat );
        //session->OnCommandCompletion( stat );
    }
    catch( std::exception &e )
    {
        PS_LOG( "AdminCommand_Stat::Execute: " << e.what() );
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

void AdminSession::RpcCall( const std::string &method, const boost::property_tree::ptree &params )
{
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
    int errCode = common::JsonRpc::Instance().HandleRequest( request_, this );
    PS_LOG( errCode );
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

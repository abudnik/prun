#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include "result_getter.h"
#include "worker_manager.h"
#include "sheduler.h"
#include "common/log.h"
#include "common/protocol.h"
#include "defines.h"

namespace master {

void ResultGetter::Run()
{
    WorkerJob workerJob;
    std::string hostIP;

    WorkerManager &workerMgr = WorkerManager::Instance();
    workerMgr.Subscribe( this );

    bool getTask = false;
    while( !stopped_ )
    {
        if ( !getTask )
        {
            boost::unique_lock< boost::mutex > lock( awakeMut_ );
            if ( !newJobAvailable_ )
                awakeCond_.wait( lock );
            newJobAvailable_ = false;
        }

        getTask = workerMgr.GetAchievedWorker( workerJob, hostIP );
        if ( getTask )
        {
            PS_LOG( "Get achieved work " << workerJob.jobId_ << " : " << workerJob.taskId_ );
            GetJobResult( workerJob, hostIP );
        }
    }
}

void ResultGetter::Stop()
{
    stopped_ = true;
    boost::unique_lock< boost::mutex > lock( awakeMut_ );
    awakeCond_.notify_all();
}

void ResultGetter::NotifyObserver( int event )
{
    boost::unique_lock< boost::mutex > lock( awakeMut_ );
    newJobAvailable_ = true;
    awakeCond_.notify_all();
}

void ResultGetter::OnGetJobResult( bool success, int errCode, const WorkerJob &workerJob, const std::string &hostIP )
{
    if ( !success ) // retrieving of job result from message failed
        errCode = -1;
    Sheduler::Instance().OnTaskCompletion( errCode, workerJob, hostIP );
}

void ResultGetterBoost::Start()
{
    io_service_.post( boost::bind( &ResultGetter::Run, this ) );
}

void ResultGetterBoost::GetJobResult( const WorkerJob &workerJob, const std::string &hostIP )
{   
    getJobsSem_.Wait();

    GetterBoost::getter_ptr getter(
        new GetterBoost( io_service_, this, workerJob, hostIP )
    );
    getter->GetJobResult();
}

void ResultGetterBoost::OnGetJobResult( bool success, int errCode, const WorkerJob &workerJob, const std::string &hostIP )
{
    getJobsSem_.Notify();
    ResultGetter::OnGetJobResult( success, errCode, workerJob, hostIP );
}

void GetterBoost::GetJobResult()
{
    tcp::endpoint nodeEndpoint(
        boost::asio::ip::address::from_string( hostIP_ ),
        NODE_PORT
    );

    socket_.async_connect( nodeEndpoint,
                           boost::bind( &GetterBoost::HandleConnect, shared_from_this(),
                                        boost::asio::placeholders::error ) );
}

void GetterBoost::HandleConnect( const boost::system::error_code &error )
{
    if ( !error )
    {
        MakeRequest();

        boost::asio::async_read( socket_,
                                 boost::asio::buffer( &buffer_, sizeof( char ) ),
                                 boost::bind( &GetterBoost::FirstRead, shared_from_this(),
                                              boost::asio::placeholders::error,
                                              boost::asio::placeholders::bytes_transferred ) );

        boost::asio::async_write( socket_,
                                  boost::asio::buffer( request_ ),
                                  boost::bind( &GetterBoost::HandleWrite, shared_from_this(),
                                               boost::asio::placeholders::error,
                                               boost::asio::placeholders::bytes_transferred ) );
    }
    else
    {
        PS_LOG( "GetterBoost::HandleConnect error=" << error.message() );
        getter_->OnGetJobResult( false, 0, workerJob_, hostIP_ );
    }
}

void GetterBoost::HandleWrite( const boost::system::error_code &error, size_t bytes_transferred )
{
    if ( error )
    {
        PS_LOG( "GetterBoost::HandleWrite error=" << error.message() );
        getter_->OnGetJobResult( false, 0, workerJob_, hostIP_ );
    }
}

void GetterBoost::FirstRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( !error )
    {
        if ( firstRead_ )
        {
            // skip node's read completion status byte
            firstRead_ = false;
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &GetterBoost::FirstRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
            return;
        }

        int ret = response_.OnFirstRead( buffer_, bytes_transferred );
        if ( ret == 0 )
        {
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &GetterBoost::FirstRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
            return;
        }
    }
    else
    {
        PS_LOG( "GetterBoost::FirstRead error=" << error.message() );
    }

    HandleRead( error, bytes_transferred );
}

void GetterBoost::HandleRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( !error )
    {
        response_.OnRead( buffer_, bytes_transferred );

        if ( !response_.IsReadCompleted() )
        {
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &GetterBoost::HandleRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
        }
        else
        {
            if ( !HandleResponse() )
            {
                getter_->OnGetJobResult( false, 0, workerJob_, hostIP_ );
            }
        }
    }
    else
    {
        PS_LOG( "GetterBoost::HandleRead error=" << error.message() );
        getter_->OnGetJobResult( false, 0, workerJob_, hostIP_ );
    }
}

bool GetterBoost::HandleResponse()
{
    const std::string &msg = response_.GetString();

    std::string protocol, header, body;
    int version;
    if ( !python_server::Protocol::ParseMsg( msg, protocol, version, header, body ) )
    {
        PS_LOG( "GetterBoost::HandleResponse: couldn't parse msg: " << msg );
        return false;
    }

    python_server::ProtocolCreator protocolCreator;
    boost::scoped_ptr< python_server::Protocol > parser(
        protocolCreator.Create( protocol, version )
    );
    if ( !parser )
    {
        PS_LOG( "GetterBoost::HandleResponse: appropriate parser not found for protocol: "
                << protocol << " " << version );
        return false;
    }

    std::string type;
    parser->ParseMsgType( header, type );
    if ( !parser->ParseMsgType( header, type ) )
    {
        PS_LOG( "GetterBoost::HandleResponse: couldn't parse msg type: " << header );
        return false;
    }

    if ( type == "send_job_result" )
    {
        int errCode;
        if ( parser->ParseJobResult( body, errCode ) )
        {
            getter_->OnGetJobResult( true, errCode, workerJob_, hostIP_ );
            return true;
        }
    }
    else
    {
        PS_LOG( "GetterBoost::HandleResponse: unexpected msg type: " << type );
    }

    return false;
}

void GetterBoost::MakeRequest()
{
    python_server::ProtocolJson protocol;
    protocol.GetJobResult( request_, workerJob_.jobId_, workerJob_.taskId_ );
}

} // namespace master

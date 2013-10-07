#include <boost/bind.hpp>
#include "job_sender.h"
#include "scheduler.h"
#include "common/log.h"
#include "common/protocol.h"
#include "defines.h"

namespace master {

void JobSender::Run()
{
    WorkerJob workerJob;
    std::string hostIP;
    Job *job;

    Scheduler &scheduler = Scheduler::Instance();
    scheduler.Subscribe( this );

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

        getTask = scheduler.GetTaskToSend( workerJob, hostIP, &job );
        if ( getTask )
        {
            PS_LOG( "Get task " << workerJob.GetJobId() );
            SendJob( workerJob, hostIP, job );
        }
    }
}

void JobSender::Stop()
{
    stopped_ = true;
    boost::unique_lock< boost::mutex > lock( awakeMut_ );
    awakeCond_.notify_all();
}

void JobSender::NotifyObserver( int event )
{
    boost::unique_lock< boost::mutex > lock( awakeMut_ );
    newJobAvailable_ = true;
    awakeCond_.notify_all();
}

void JobSender::OnJobSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const Job *job )
{
    PS_LOG("JobSender::OnJobSendCompletion "<<success);
    Scheduler::Instance().OnTaskSendCompletion( success, workerJob, hostIP, job );
    if ( success )
    {
        // problem: if taskId == 0 rescheduled, then job timeout will be pushed twice
        if ( !workerJob.taskId_ )
        {
            // first task send completion means that job execution started
            timeoutManager_->PushJob( workerJob.GetJobId(), job->GetTimeout() );
        }
        timeoutManager_->PushTask( workerJob, hostIP, job->GetTaskTimeout() );
    }
}

void JobSenderBoost::Start()
{
    io_service_.post( boost::bind( &JobSender::Run, this ) );
}

void JobSenderBoost::SendJob( const WorkerJob &workerJob, const std::string &hostIP, const Job *job )
{   
    sendJobsSem_.Wait();

    SenderBoost::sender_ptr sender(
        new SenderBoost( io_service_, sendBufferSize_, this, workerJob, hostIP, job )
    );
    sender->Send();
}

void JobSenderBoost::OnJobSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const Job *job )
{
    sendJobsSem_.Notify();
    JobSender::OnJobSendCompletion( success, workerJob, hostIP, job );
}

void SenderBoost::Send()
{
    tcp::endpoint nodeEndpoint(
        boost::asio::ip::address::from_string( hostIP_ ),
        NODE_PORT
    );

    socket_.async_connect( nodeEndpoint,
                           boost::bind( &SenderBoost::HandleConnect, shared_from_this(),
                                        boost::asio::placeholders::error ) );
}

void SenderBoost::HandleConnect( const boost::system::error_code &error )
{
    if ( !error )
    {
        MakeRequest();

        boost::asio::async_read( socket_,
                                 boost::asio::buffer( &response_, sizeof( response_ ) ),
                                 boost::bind( &SenderBoost::HandleRead, shared_from_this(),
                                              boost::asio::placeholders::error,
                                              boost::asio::placeholders::bytes_transferred ) );

        boost::asio::async_write( socket_,
                                  boost::asio::buffer( request_ ),
                                  boost::bind( &SenderBoost::HandleWrite, shared_from_this(),
                                               boost::asio::placeholders::error,
                                               boost::asio::placeholders::bytes_transferred ) );
    }
    else
    {
        PS_LOG( "SenderBoost::HandleConnect error=" << error.message() );
        sender_->OnJobSendCompletion( false, workerJob_, hostIP_, job_ );
    }
}

void SenderBoost::HandleWrite( const boost::system::error_code &error, size_t bytes_transferred )
{
    if ( error )
    {
        PS_LOG( "SenderBoost::HandleWrite error=" << error.message() );
        sender_->OnJobSendCompletion( false, workerJob_, hostIP_, job_ );
    }
}

void SenderBoost::HandleRead( const boost::system::error_code &error, size_t bytes_transferred )
{
    if ( !error )
    {
        bool success = ( response_ == '1' );
        sender_->OnJobSendCompletion( success, workerJob_, hostIP_, job_ );
    }
    else
    {
        PS_LOG( "SenderBoost::HandleRead error=" << error.message() );
        sender_->OnJobSendCompletion( false, workerJob_, hostIP_, job_ );
    }
}

void SenderBoost::MakeRequest()
{
    python_server::ProtocolJson protocol;
    protocol.SendScript( request_, job_->GetScriptLanguage(), job_->GetScript(),
                         workerJob_.GetJobId(), workerJob_.GetTasks(),
                         job_->GetNumPlannedExec(), job_->GetTaskTimeout() );
}

} // namespace master

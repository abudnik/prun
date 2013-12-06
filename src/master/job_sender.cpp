#include <boost/bind.hpp>
#include "job_sender.h"
#include "scheduler.h"
#include "job_manager.h"
#include "common/log.h"
#include "common/protocol.h"
#include "defines.h"

namespace master {

void JobSender::Run()
{
    WorkerJob workerJob;
    std::string hostIP;
    JobPtr job;

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

        getTask = scheduler.GetTaskToSend( workerJob, hostIP, job );
        if ( getTask )
        {
            PLOG( "Get task " << workerJob.GetJobId() );
            SendJob( workerJob, hostIP, job );
            workerJob.Reset();
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

void JobSender::OnJobSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const JobPtr &job )
{
    PLOG("JobSender::OnJobSendCompletion "<<success);
    Scheduler::Instance().OnTaskSendCompletion( success, workerJob, hostIP, job );
    if ( success )
    {
        WorkerJob::Tasks tasks;
        workerJob.GetTasks( workerJob.GetJobId(), tasks );
        WorkerJob::Tasks::const_iterator it = tasks.begin();
        for( ; it != tasks.end(); ++it )
        {
            int taskId = *it;
            if ( taskId == 0 )
            {
                // First task send completion means that job execution started.
                // Even if task with id == 0 rescheduled, overall job timeout
                // behavior will be normal
                timeoutManager_->PushJob( workerJob.GetJobId(), job->GetTimeout() );
            }
            timeoutManager_->PushTask( WorkerTask( workerJob.GetJobId(), taskId ),
                                       hostIP, job->GetTaskTimeout() );
        }
    }
}

void JobSenderBoost::Start()
{
    io_service_.post( boost::bind( &JobSender::Run, this ) );
}

void JobSenderBoost::Stop()
{
    JobSender::Stop();

    sendJobsSem_.Reset();
}

void JobSenderBoost::SendJob( const WorkerJob &workerJob, const std::string &hostIP, JobPtr &job )
{   
    sendJobsSem_.Wait();

    SenderBoost::sender_ptr sender(
        new SenderBoost( io_service_, this, workerJob, hostIP, job )
    );
    sender->Send();
}

void JobSenderBoost::OnJobSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const JobPtr &job )
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
        PLOG( "SenderBoost::HandleConnect error=" << error.message() );
        sender_->OnJobSendCompletion( false, workerJob_, hostIP_, job_ );
    }
}

void SenderBoost::HandleWrite( const boost::system::error_code &error, size_t bytes_transferred )
{
    if ( error )
    {
        PLOG( "SenderBoost::HandleWrite error=" << error.message() );
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
        PLOG( "SenderBoost::HandleRead error=" << error.message() );
        sender_->OnJobSendCompletion( false, workerJob_, hostIP_, job_ );
    }
}

void SenderBoost::MakeRequest()
{
    WorkerJob::Tasks tasks;
    workerJob_.GetTasks( workerJob_.GetJobId(), tasks );

    const std::string &masterId = JobManager::Instance().GetMasterId();

    common::ProtocolJson protocol;
    protocol.SendScript( request_, job_->GetScriptLanguage(),
                         job_->GetScript(), job_->GetFilePath(),
                         masterId, workerJob_.GetJobId(), tasks,
                         job_->GetNumPlannedExec(), job_->GetTaskTimeout() );
}

} // namespace master

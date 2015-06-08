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

#include <boost/bind.hpp>
#include "job_sender.h"
#include "scheduler.h"
#include "job_manager.h"
#include "common/log.h"
#include "common/config.h"
#include "common/protocol.h"
#include "common/service_locator.h"

namespace master {

void JobSender::Run()
{
    WorkerJob workerJob;
    std::string hostIP;
    JobPtr job;

    IScheduler *scheduler = common::GetService< IScheduler >();
    scheduler->Subscribe( this );

    bool getTask = false;
    while( !stopped_ )
    {
        if ( !getTask )
        {
            std::unique_lock< std::mutex > lock( awakeMut_ );
            if ( !newJobAvailable_ )
                awakeCond_.wait( lock );
            newJobAvailable_ = false;
        }

        getTask = scheduler->GetTaskToSend( workerJob, hostIP, job );
        if ( getTask )
        {
            PLOG( "Send job " << workerJob.GetJobId() << " to " << hostIP );
            SendJob( workerJob, hostIP, job );
            workerJob.Reset();
        }
    }
}

void JobSender::Stop()
{
    stopped_ = true;
    std::unique_lock< std::mutex > lock( awakeMut_ );
    awakeCond_.notify_all();
}

void JobSender::NotifyObserver( int event )
{
    std::unique_lock< std::mutex > lock( awakeMut_ );
    newJobAvailable_ = true;
    awakeCond_.notify_all();
}

void JobSender::OnJobSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const JobPtr &job )
{
    PLOG( "JobSender::OnJobSendCompletion: host=" << hostIP << ", result=" << success );
    IScheduler *scheduler = common::GetService< IScheduler >();
    scheduler->OnTaskSendCompletion( success, workerJob, hostIP );
    if ( success )
    {
        WorkerJob::Tasks tasks;
        workerJob.GetTasks( workerJob.GetJobId(), tasks );
        for( auto taskId : tasks )
        {
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
    const common::Config &cfg = common::Config::Instance();
    const unsigned short node_port = cfg.Get<unsigned short>( "node_port" );

    tcp::endpoint nodeEndpoint(
        boost::asio::ip::address::from_string( hostIP_ ),
        node_port
    );

    socket_.async_connect( nodeEndpoint,
                           boost::bind( &SenderBoost::HandleConnect, shared_from_this(),
                                        boost::asio::placeholders::error ) );
}

void SenderBoost::HandleConnect( const boost::system::error_code &error )
{
    if ( !error )
    {
        boost::system::error_code ec;
        socket_.set_option( tcp::no_delay( true ), ec );

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
        PLOG_WRN( "SenderBoost::HandleConnect error=" << error.message() );
        OnCompletion( false );
    }
}

void SenderBoost::HandleWrite( const boost::system::error_code &error, size_t bytes_transferred )
{
    if ( error )
    {
        PLOG_WRN( "SenderBoost::HandleWrite error=" << error.message() );
        OnCompletion( false );
    }
}

void SenderBoost::HandleRead( const boost::system::error_code &error, size_t bytes_transferred )
{
    if ( !error )
    {
        bool success = ( response_ == '1' );
        OnCompletion( success );
        WaitClientDisconnect();
    }
    else
    {
        PLOG_WRN( "SenderBoost::HandleRead error=" << error.message() );
        OnCompletion( false );
    }
}

void SenderBoost::WaitClientDisconnect()
{
    boost::asio::async_read( socket_,
                             boost::asio::buffer( &response_, sizeof( response_ ) ),
                             boost::bind( &SenderBoost::HandleLastRead, shared_from_this(),
                                          boost::asio::placeholders::error,
                                          boost::asio::placeholders::bytes_transferred ) );
}

void SenderBoost::HandleLastRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    // if not yet disconnected from opposite side
    if ( !error ) WaitClientDisconnect();
}

void SenderBoost::OnCompletion( bool success )
{
    {
        std::unique_lock< std::mutex > lock( completionMut_ );
        if ( completed_ )
            return;
        else
            completed_ = true;
    }

    sender_->OnJobSendCompletion( success, workerJob_, hostIP_, job_ );
}

void SenderBoost::MakeRequest()
{
    WorkerJob::Tasks tasks;
    workerJob_.GetTasks( workerJob_.GetJobId(), tasks );

    IJobManager *jobManager = common::GetService< IJobManager >();
    const std::string &masterId = jobManager->GetMasterId();

    common::Marshaller marshaller;
    marshaller( "lang", job_->GetScriptLanguage() )
        ( "script", job_->GetScript() )
        ( "file_path", job_->GetFilePath() )
        ( "master_id", masterId )
        ( "job_id", workerJob_.GetJobId() )
        ( "num_tasks", job_->GetNumPlannedExec() )
        ( "timeout", job_->GetTaskTimeout() )
        ( "tasks", tasks );

    common::ProtocolJson protocol;
    protocol.Serialize( request_, "exec", marshaller );

    // cratch for boost bug with unexepected whitespaces in arrays:  "val": [   <whitespaces> ]
    request_.erase( std::remove_if( request_.begin(), request_.end(), common::IsBlank ), request_.end() );
}

} // namespace master

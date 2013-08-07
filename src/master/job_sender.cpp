#include <boost/bind.hpp>
#include "job_sender.h"
#include "sheduler.h"
#include "common/log.h"
#include "common/protocol.h"
#include "defines.h"

namespace master {

void JobSender::Run()
{
    Worker *worker;
    Job *job;

    Sheduler &sheduler = Sheduler::Instance();
	sheduler.Subscribe( this );

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

        getTask = sheduler.GetTaskToSend( &worker, &job );
		if ( getTask )
		{
			const WorkerJob &j = worker->GetJob();
			PS_LOG( "Get task " << j.jobId_ << " : " << j.taskId_ );
			SendJob( worker, job );
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

void JobSender::OnJobSendCompletion( bool success, const Worker *worker, const Job *job )
{
    PS_LOG("JobSender::OnJobSendCompletion "<<success);
}

void JobSenderBoost::Start()
{
    io_service_.post( boost::bind( &JobSender::Run, this ) );
}

void JobSenderBoost::SendJob( const Worker *worker, const Job *job )
{	
	sendJobsSem_.Wait();

	SenderBoost::sender_ptr sender(
		new SenderBoost( io_service_, sendBufferSize_, this, worker, job )
	);
	sender->Send();
}

void JobSenderBoost::OnJobSendCompletion( bool success, const Worker *worker, const Job *job )
{
    sendJobsSem_.Notify();
    JobSender::OnJobSendCompletion( success, worker, job );
}

void SenderBoost::Send()
{
	tcp::endpoint nodeEndpoint(
		boost::asio::ip::address::from_string( worker_->GetIP() ),
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
        boost::asio::async_write( socket_,
                                  boost::asio::buffer( request_ ),
                                  boost::bind( &SenderBoost::HandleWrite, shared_from_this(),
                                               boost::asio::placeholders::error,
                                               boost::asio::placeholders::bytes_transferred ) );
	}
	else
	{
		PS_LOG( "SenderBoost::HandleConnect error=" << error );
		sender_->OnJobSendCompletion( false, worker_, job_ );
	}
}

void SenderBoost::HandleWrite( const boost::system::error_code &error, size_t bytes_transferred )
{
    if ( !error )
    {
        boost::asio::async_read( socket_,
                                 boost::asio::buffer( &response_, sizeof( response_ ) ),
                                 boost::bind( &SenderBoost::HandleRead, shared_from_this(),
                                              boost::asio::placeholders::error,
                                              boost::asio::placeholders::bytes_transferred ) );
    }
    else
    {
        PS_LOG( "SenderBoost::HandleWrite error=" << error.value() );
        sender_->OnJobSendCompletion( false, worker_, job_ );
    }
}

void SenderBoost::HandleRead( const boost::system::error_code &error, size_t bytes_transferred )
{
    if ( !error )
    {
        bool success = ( response_ == '1' );
        sender_->OnJobSendCompletion( success, worker_, job_ );
    }
    else
    {
        PS_LOG( "SenderBoost::HandleRead error=" << error.value() );
        sender_->OnJobSendCompletion( false, worker_, job_ );
    }
}

void SenderBoost::MakeRequest()
{
    python_server::ProtocolJson protocol;
    protocol.SendScript( request_, job_->GetScriptLanguage(), job_->GetScript() );
}

} // namespace master

#include <boost/bind.hpp>
#include "job_sender.h"
#include "sheduler.h"
#include "common/log.h"

namespace master {

void JobSender::Run()
{
    Worker *worker;
    Job *job;

    Sheduler &sheduler = Sheduler::Instance();
	sheduler.Subscribe( this );

    while( !stopped_ )
    {
		{
			boost::unique_lock< boost::mutex > lock( awakeMut_ );
			if ( !newJobAvailable_ )
				awakeCond_.wait( lock );
		}

		if ( sheduler.GetTaskToSend( &worker, &job ) )
		{
			const WorkerJob &j = worker->GetJob();
			PS_LOG( "Get task " << j.jobId_ << " : " << j.taskId_ );
			SendJob( worker, job );
		}
		else
		{
			boost::unique_lock< boost::mutex > lock( awakeMut_ );
			newJobAvailable_ = false;
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

void JobSenderBoost::Start()
{
    io_service_.post( boost::bind( &JobSender::Run, this ) );
}

void JobSenderBoost::SendJob( const Worker *worker, const Job *job )
{	
	sendJobsSem_.Wait();
	sendJobsSem_.Notify();
}

} // namespace master

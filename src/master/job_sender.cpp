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
        if ( sheduler.GetTaskToSend( &worker, &job ) )
        {
            const WorkerJob &j = worker->GetJob();
            PS_LOG( "Get task " << j.jobId_ << " : " << j.taskId_ );
        }
    }
}

void JobSender::Stop()
{
    stopped_ = true;
}

void JobSender::NotifyObserver( int event )
{
}

void JobSenderBoost::Start()
{
    io_service_.post( boost::bind( &JobSender::Run, this ) );
}

} // namespace master

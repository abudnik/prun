#include <boost/bind.hpp>
#include "job_timeout.h"

namespace master {

void JobTimeout::Start()
{
    io_service_.post( boost::bind( &JobTimeout::Run, this ) );
}

void JobTimeout::Stop()
{
    stopped_ = true;
    timer_.StopWaiting();
}

void JobTimeout::Run()
{
    while( !stopped_ )
    {
        timer_.Wait( 1000 );
        CheckTimeouts();
    }
}

void JobTimeout::CheckTimeouts()
{
    namespace pt = boost::posix_time;
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    TimeToJob::const_iterator it = jobs_.begin();
    const pt::ptime now = pt::second_clock::local_time();
    for( ; it != jobs_.end(); ++it )
    {
        if ( now > it->first )
        {
        }
    }
}

void JobTimeout::PushJob( const WorkerJob &job, int timeout )
{
    namespace pt = boost::posix_time;
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    const pt::ptime now = pt::second_clock::local_time();
    const pt::ptime deadline = now + pt::milliseconds( timeout );
    jobs_[ deadline ] = job;
}

} // namespace master

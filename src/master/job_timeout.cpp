#include <boost/bind.hpp>
#include "job_timeout.h"
#include "sheduler.h"

namespace master {

void JobTimeoutManager::Start()
{
    io_service_.post( boost::bind( &JobTimeoutManager::Run, this ) );
}

void JobTimeoutManager::Stop()
{
    stopped_ = true;
    timer_.StopWaiting();
}

void JobTimeoutManager::Run()
{
    while( !stopped_ )
    {
        timer_.Wait( 1000 );
        CheckTimeouts();
    }
}

void JobTimeoutManager::CheckTimeouts()
{
    namespace pt = boost::posix_time;
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    TimeToJob::iterator it = jobs_.begin();
    const pt::ptime now = pt::second_clock::local_time();
    for( ; it != jobs_.end(); )
    {
        const pt::ptime &jobSendTime = it->first;
        if ( now < jobSendTime ) // skip earlier sended jobs
            break;

        const JobPair &pair = it->second;
        Sheduler::Instance().OnJobTimeout( pair.first, pair.second );
        jobs_.erase( it++ );
    }
}

void JobTimeoutManager::PushJob( const WorkerJob &job, const std::string &hostIP, int timeout )
{
    namespace pt = boost::posix_time;
    const pt::ptime now = pt::second_clock::local_time();
    const pt::ptime deadline = now + pt::seconds( timeout );
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    jobs_.insert( std::pair< pt::ptime, JobPair >(
                      deadline,
                      std::pair< WorkerJob, std::string >( job, hostIP )
                      )
    );
}

} // namespace master

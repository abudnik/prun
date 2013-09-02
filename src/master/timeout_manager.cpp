#include <boost/bind.hpp>
#include "timeout_manager.h"
#include "sheduler.h"

namespace master {

void TimeoutManager::TaskTimeoutHandler::HandleTimeout()
{
    Sheduler::Instance().OnTaskTimeout( workerJob_, hostIP_ );
}

void TimeoutManager::JobTimeoutHandler::HandleTimeout()
{
    Sheduler::Instance().OnJobTimeout( jobId_ );
}

void TimeoutManager::JobQueueTimeoutHandler::HandleTimeout()
{
    Sheduler::Instance().OnJobQueueTimeout( jobId_ );
}


void TimeoutManager::Start()
{
    io_service_.post( boost::bind( &TimeoutManager::Run, this ) );
}

void TimeoutManager::Stop()
{
    stopped_ = true;
    timer_.StopWaiting();
}

void TimeoutManager::Run()
{
    while( !stopped_ )
    {
        timer_.Wait( 1000 );
        CheckTimeouts();
    }
}

void TimeoutManager::CheckTimeouts()
{
    namespace pt = boost::posix_time;
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    TimeToCallback::iterator it = jobs_.begin();
    const pt::ptime now = pt::second_clock::local_time();
    for( ; it != jobs_.end(); )
    {
        const pt::ptime &jobSendTime = it->first;
        if ( now < jobSendTime ) // skip earlier sended jobs
            break;

        Callback callback( it->second );
        callback();
        jobs_.erase( it++ );
    }
}

void TimeoutManager::PushTask( const WorkerJob &job, const std::string &hostIP, int timeout )
{
    namespace pt = boost::posix_time;
    const pt::ptime now = pt::second_clock::local_time();
    const pt::ptime deadline = now + pt::seconds( timeout );

    boost::shared_ptr< TaskTimeoutHandler > handler( new TaskTimeoutHandler );
    handler->workerJob_ = job;
    handler->hostIP_ = hostIP;
    Callback callback(
        boost::bind( &TaskTimeoutHandler::HandleTimeout, handler )
    );

    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    jobs_.insert( std::pair< pt::ptime, Callback >(
                      deadline,
                      callback
                )
    );
}

void TimeoutManager::PushJob( int64_t jobId, int jobTimeout, int queueTimeout )
{
    namespace pt = boost::posix_time;
    const pt::ptime now = pt::second_clock::local_time();
    const pt::ptime deadline = now + pt::seconds( jobTimeout );
    const pt::ptime deadlineQueue = now + pt::seconds( queueTimeout );

    boost::shared_ptr< JobTimeoutHandler > handler( new JobTimeoutHandler );
    handler->jobId_ = jobId;
    Callback callback(
        boost::bind( &JobTimeoutHandler::HandleTimeout, handler )
    );

    boost::shared_ptr< JobQueueTimeoutHandler > handlerQueue( new JobQueueTimeoutHandler );
    handlerQueue->jobId_ = jobId;
    Callback callbackQueue(
        boost::bind( &JobQueueTimeoutHandler::HandleTimeout, handlerQueue )
    );

    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    jobs_.insert( std::pair< pt::ptime, Callback >(
                      deadline,
                      callback
                )
    );
    jobs_.insert( std::pair< pt::ptime, Callback >(
                      deadlineQueue,
                      callbackQueue
                )
    );
}

} // namespace master

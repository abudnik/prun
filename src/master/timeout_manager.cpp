#include <boost/bind.hpp>
#include "timeout_manager.h"
#include "scheduler.h"
#include "job_manager.h"

namespace master {

void TimeoutManager::TaskTimeoutHandler::HandleTimeout()
{
    Scheduler::Instance().OnTaskTimeout( workerJob_, hostIP_ );
}

void TimeoutManager::JobTimeoutHandler::HandleTimeout()
{
    Scheduler::Instance().OnJobTimeout( jobId_ );
}

void TimeoutManager::JobQueueTimeoutHandler::HandleTimeout()
{
    JobManager::Instance().DeleteJob( jobId_ );
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

void TimeoutManager::PushJobQueue( int64_t jobId, int queueTimeout )
{
    if ( queueTimeout < 0 )
        return;

    namespace pt = boost::posix_time;
    const pt::ptime now = pt::second_clock::local_time();
    const pt::ptime deadlineQueue = now + pt::seconds( queueTimeout );

    boost::shared_ptr< JobQueueTimeoutHandler > handlerQueue( new JobQueueTimeoutHandler );
    handlerQueue->jobId_ = jobId;
    Callback callbackQueue(
        boost::bind( &JobQueueTimeoutHandler::HandleTimeout, handlerQueue )
    );

    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    jobs_.insert( std::pair< pt::ptime, Callback >(
                      deadlineQueue,
                      callbackQueue
                )
    );
}

void TimeoutManager::PushJob( int64_t jobId, int jobTimeout )
{
    if ( jobTimeout < 0 )
        return;

    namespace pt = boost::posix_time;
    const pt::ptime now = pt::second_clock::local_time();
    const pt::ptime deadline = now + pt::seconds( jobTimeout );

    boost::shared_ptr< JobTimeoutHandler > handler( new JobTimeoutHandler );
    handler->jobId_ = jobId;
    Callback callback(
        boost::bind( &JobTimeoutHandler::HandleTimeout, handler )
    );

    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    jobs_.insert( std::pair< pt::ptime, Callback >(
                      deadline,
                      callback
                )
    );
}

void TimeoutManager::PushTask( const WorkerJob &job, const std::string &hostIP, int timeout )
{
    if ( timeout < 0 )
        return;

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

} // namespace master

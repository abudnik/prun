#include "job.h"
#include "common/log.h"

namespace master {

void JobQueue::PushJob( Job *job )
{
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    jobs_.push_back( job );
    idToJob_[ job->GetJobId() ] = job;
    ++numJobs_;
}

Job *JobQueue::GetJobById( int64_t jobId )
{
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    IdToJob::const_iterator it = idToJob_.find( jobId );
    if ( it != idToJob_.end() )
        return it->second;
    return NULL;
}

bool JobQueue::DeleteJob( int64_t jobId )
{
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );

    {
        IdToJob::iterator it = idToJob_.find( jobId );
        if ( it == idToJob_.end() )
            return false;
        idToJob_.erase( it );
    }

    std::list< Job * >::iterator it = jobs_.begin();
    for( ; it != jobs_.end(); ++it )
    {
        Job *job = *it;
        std::ostringstream ss;
        ss << "================" << std::endl <<
            "Job deleted from job queue, jobId = " << job->GetJobId() << std::endl <<
            "completion status: failed" << std::endl <<
            "================";

        PS_LOG( ss.str() );

        job->RunCallback( ss.str() );
        delete job;

        jobs_.erase( it );
        --numJobs_;
        return true;
    }
    return false;
}

Job *JobQueue::PopJob()
{
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    if ( numJobs_ )
    {
        Job *job = jobs_.front();
        jobs_.pop_front();
        idToJob_.erase( job->GetJobId() );
        --numJobs_;
        return job;
    }
    return NULL;
}

Job *JobQueue::GetTopJob()
{
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    if ( numJobs_ )
        return jobs_.front();
    return NULL;
}

void JobQueue::Clear( bool doDelete )
{
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    if ( doDelete )
    {
        std::list< Job * >::iterator it = jobs_.begin();
        for( ; it != jobs_.end(); ++it )
        {
            delete *it;
        }
    }
    jobs_.clear();
    idToJob_.clear();
    numJobs_ = 0;
}

} // namespace master

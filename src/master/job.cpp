#include "job.h"

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

Job *JobQueue::PopJob()
{
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    if ( numJobs_ )
    {
        Job *j = jobs_.front();
        jobs_.pop_front();
        --numJobs_;
        return j;
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
    numJobs_ = 0;
}

} // namespace master

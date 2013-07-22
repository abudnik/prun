#include "job.h"

namespace master {

void JobQueue::PushJob( Job *job )
{
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    jobs_.push_back( job );
    ++numJobs_;
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

} // namespace master

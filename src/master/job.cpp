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

void JobQueue::Clear( bool doDelete )
{
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

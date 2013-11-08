#include "job.h"
#include "common/log.h"

namespace master {

void JobGroup::OnJobCompletion()
{
    if ( currentRank_ > (int)jobExec_.size() )
        return;
    if ( --jobExec_[ currentRank_ ] <= 0 )
        ++currentRank_;
}

void JobGroup::IncrementRank( int rank )
{
    if ( rank + 1 > (int)jobExec_.size() )
        jobExec_.resize( rank + 1, 0 );
    ++jobExec_[ rank ];
}

void JobQueue::PushJob( Job *job, int64_t groupId )
{
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    job->SetGroupId( groupId );
    jobs_.push_back( job );
    idToJob_[ job->GetJobId() ] = job;
    ++numJobs_;
}

void JobQueue::PushJobs( std::list< Job * > &jobs, int64_t groupId )
{
    boost::mutex::scoped_lock scoped_lock( jobsMut_ );
    std::list< Job * >::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        Job *job = *it;
        job->SetGroupId( groupId );
        jobs_.push_back( job );
        idToJob_[ job->GetJobId() ] = job;
        ++numJobs_;
    }
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
        std::list< Job * > jobs;
        Sort( jobs );

        std::list< Job * >::const_iterator it = jobs.begin();
        for( ; it != jobs.end(); ++it )
        {
            Job *job = *it;
            if ( job->GetCurrentRank() < job->GetRank() )
                continue;

            std::list< Job * >::iterator i = jobs_.begin();
            for( ; i != jobs_.end(); ++i )
            {
                if ( job == *i )
                {
                    jobs_.erase( i );
                    break;
                }
            }

            idToJob_.erase( job->GetJobId() );
            --numJobs_;
            return job;
        }
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

struct JobComparatorGroup
{
    bool operator() ( const Job *a, const Job *b ) const
    {
        if ( a->GetGroupId() < b->GetGroupId() )
            return true;
        if ( a->GetGroupId() == b->GetGroupId() )
        {
            if ( a->GetRank() < b->GetRank() )
                return true;
        }
        return false;
    }
};

struct JobComparatorPriority
{
    bool operator() ( const Job *a, const Job *b ) const
    {
        if ( a->GetPriority() < b->GetPriority() )
            return true;
        if ( a->GetPriority() == b->GetPriority() )
        {
            if ( a->GetGroupId() < b->GetGroupId() )
                return true;
        }
        return false;
    }
};

void JobQueue::Sort( std::list< Job * > &jobs )
{
    jobs_.sort( JobComparatorGroup() );
    std::list< Job * >::const_iterator it = jobs_.begin();
    Job *job = *it;
    int64_t groupId = job->GetGroupId();
    int rank = job->GetRank();
    // fill jobs list with most ranked jobs from each job group
    for( ; it != jobs_.end(); ++it )
    {
        job = *it;
        if ( groupId != job->GetGroupId() )
        {
            groupId = job->GetGroupId();
            rank = job->GetRank();
            jobs.push_back( job );
        }
        else
        {
            if ( rank == job->GetRank() )
            {
                jobs.push_back( job );
            }
        }
    }

    // sort jobs by priority, saving group order
    jobs.sort( JobComparatorPriority() );
    PrintJobs( jobs );
}

void JobQueue::PrintJobs( const std::list< Job * > &jobs ) const
{
    std::ostringstream ss;
    ss << std::endl;
    std::list< Job * >::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        if ( it != jobs.begin() )
            ss << "," << std::endl;
        ss << "(priority=" << (*it)->GetPriority() << ", groupid=" <<
            (*it)->GetGroupId() << ", rank=" << (*it)->GetRank() << ")";
    }
    PS_LOG( ss.str() );
}

} // namespace master

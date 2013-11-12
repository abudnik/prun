#ifndef __SCHEDULED_JOBS_H
#define __SCHEDULED_JOBS_H

#include <list>
#include <map>
#include "job.h"
#include "common/log.h"

namespace master {

class ScheduledJobs
{
public:
    typedef std::list< Job * > JobList;

public:
    void Add( Job *job, int numExec )
    {
        jobExecutions_[ job->GetJobId() ] = numExec;
        jobs_.push_back( job );
    }

    void DecrementJobExecution( int64_t jobId, int numTasks )
    {
        int numExecution = jobExecutions_[ jobId ] - numTasks;
        jobExecutions_[ jobId ] = numExecution;
        if ( numExecution < 1 )
        {
            RemoveJob( jobId, "success" );
        }
    }

    Job *FindJobByJobId( int64_t jobId ) const
    {
        JobList::const_iterator it = jobs_.begin();
        for( ; it != jobs_.end(); ++it )
        {
            Job *job = *it;
            if ( job->GetJobId() == jobId )
                return job;
        }
        return NULL;
    }

    void GetJobGroup( int64_t groupId, std::list< Job * > &jobs ) const
    {
        JobList::const_iterator it = jobs_.begin();
        for( ; it != jobs_.end(); ++it )
        {
            Job *job = *it;
            if ( job->GetGroupId() == groupId )
                jobs.push_back( job );
        }
    }

    int GetNumExec( int64_t jobId ) const
    {
        std::map< int64_t, int >::const_iterator it = jobExecutions_.find( jobId );
        if ( it != jobExecutions_.end() )
        {
            return it->second;
        }
        return -1;
    }

    size_t GetNumJobs() const { return jobs_.size(); }
    const JobList &GetJobList() const { return jobs_; }

    template< typename T >
    void SetOnRemoveCallback( T *obj, void (T::*f)( int64_t jobId ) )
    {
        onRemoveCallback_ = boost::bind( f, obj, _1 );
    }

    void RemoveJob( int64_t jobId, const char *completionStatus )
    {
        if ( onRemoveCallback_ )
            onRemoveCallback_( jobId );

        jobExecutions_.erase( jobId );
        JobList::iterator it = jobs_.begin();
        for( ; it != jobs_.end(); ++it )
        {
            Job *job = *it;
            if ( job->GetJobId() == jobId )
            {
                RunJobCallback( job, completionStatus );
                jobs_.erase( it );
                delete job;
                return;
            }
        }

        PS_LOG( "ScheduledJobs::RemoveJob: job not found for jobId=" << jobId );
    }

    void Clear()
    {
        while( !jobs_.empty() )
        {
            delete jobs_.front();
            jobs_.pop_front();
        }
    }

private:
    void RunJobCallback( Job *job, const char *completionStatus )
    {
        std::ostringstream ss;
        ss << "================" << std::endl <<
            "Job completed, jobId = " << job->GetJobId() << std::endl <<
            "completion status: " << completionStatus << std::endl <<
            "================";

        PS_LOG( ss.str() );

        boost::property_tree::ptree params;
        params.put( "job_id", job->GetJobId() );
        params.put( "status", completionStatus );

        job->RunCallback( "on_job_completion", params );
    }

private:
    JobList jobs_;
    std::map< int64_t, int > jobExecutions_; // job_id -> num job remaining executions (== 0, if job execution completed)
    boost::function< void (int64_t) > onRemoveCallback_;
};

} // namespace master

#endif

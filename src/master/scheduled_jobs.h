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
    typedef std::list< JobPtr > JobList;

public:
    void Add( JobPtr &job, int numExec )
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

    bool FindJobByJobId( int64_t jobId, JobPtr &job )
    {
        JobList::iterator it = jobs_.begin();
        for( ; it != jobs_.end(); ++it )
        {
            JobPtr &j = *it;
            if ( j->GetJobId() == jobId )
            {
                job = j;
                return true;
            }
        }
        return false;
    }

    void GetJobGroup( int64_t groupId, std::list< JobPtr > &jobs )
    {
        JobList::iterator it = jobs_.begin();
        for( ; it != jobs_.end(); ++it )
        {
            JobPtr &job = *it;
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
            JobPtr &job = *it;
            if ( job->GetJobId() == jobId )
            {
                RunJobCallback( job, completionStatus );
                job->ReleaseJobGroup();
                jobs_.erase( it );
                return;
            }
        }

        PLOG( "ScheduledJobs::RemoveJob: job not found for jobId=" << jobId );
    }

    void Clear()
    {
        jobs_.clear();
    }

private:
    void RunJobCallback( JobPtr &job, const char *completionStatus )
    {
        std::ostringstream ss;
        ss << std::endl << "================" << std::endl <<
            "Job completed, jobId = " << job->GetJobId() << std::endl <<
            "completion status: " << completionStatus << std::endl <<
            "================" << std::endl;

        PLOG( ss.str() );

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

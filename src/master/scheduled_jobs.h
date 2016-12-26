/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2013 Andrey Budnik <budnik27@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

===========================================================================
*/

#ifndef __SCHEDULED_JOBS_H
#define __SCHEDULED_JOBS_H

#include <set>
#include <map>
#include <boost/bimap/bimap.hpp>
#include <boost/bimap/multiset_of.hpp>
#include "job.h"
#include "cron_manager.h"
#include "job_manager.h"
#include "common/service_locator.h"
#include "common/log.h"

namespace master {

class JobState
{
public:
    JobState( JobPtr &job ) : job_( job ), sendedCompletely_( false ) {}
    JobState() : sendedCompletely_( false ) {}

    const JobPtr &GetJob() const { return job_; }

    bool IsSendedCompletely() const { return sendedCompletely_; }
    void SetSendedCompletely( bool v ) { sendedCompletely_ = v; }

    bool operator < ( const JobState &jobState ) const
    {
        static JobComparatorPriority comparator;
        return comparator( jobState.GetJob(), job_ );
    }

private:
    JobPtr job_;
    bool sendedCompletely_;
};

using namespace boost::bimaps;

class ScheduledJobs
{
private:
    typedef std::map< int64_t, int > IdToJobExec;
    typedef std::multimap< std::string, int64_t > JobNameToJob;

public:
    typedef bimap< set_of< int64_t >, multiset_of< JobState > > JobPriorityQueue; // job_id -> JobState
    typedef JobPriorityQueue::right_map::const_iterator JobIterator;

public:
    void Add( JobPtr &job, int numExec )
    {
        jobExecutions_[ job->GetJobId() ] = numExec;

        if ( !job->GetName().empty() )
        {
            nameToJob_.emplace( job->GetName(), job->GetJobId() );
        }
        if ( job->GetJobGroup() )
        {
            const std::string &metaJobName = job->GetJobGroup()->GetName();
            if ( !metaJobName.empty() )
            {
                nameToJob_.emplace( metaJobName, job->GetJobId() );
            }
        }

        typedef JobPriorityQueue::value_type value_type;
        jobs_.insert( value_type( job->GetJobId(), JobState( job ) ) );
    }

    void DecrementJobExecution( int64_t jobId, int numTasks, bool success )
    {
        auto it = jobExecutions_.find( jobId );
        if ( it != jobExecutions_.end() )
        {
            const int numExecution = it->second - numTasks;
            it->second = numExecution;
            if ( numExecution < 1 )
            {
                RemoveJob( jobId, success, success ? "success" : "failure" );
            }
        }
    }

    bool FindJobByJobId( int64_t jobId, JobPtr &job ) const
    {
        auto it = jobs_.left.find( jobId );
        if ( it != jobs_.left.end() )
        {
            const JobState &jobState = it->second;
            job = jobState.GetJob();
            return true;
        }

        return false;
    }

    void GetJobGroup( int64_t groupId, std::list< JobPtr > &jobs ) const
    {
        for( auto it = jobs_.left.begin(); it != jobs_.left.end(); ++it )
        {
            const JobState &jobState = it->second;
            const JobPtr &job = jobState.GetJob();
            if ( job->GetGroupId() == groupId )
                jobs.push_back( job );
        }
    }

    void GetJobsByName( const std::string &name, std::set< int64_t > &jobs )
    {
        auto it_low = nameToJob_.lower_bound( name );
        auto it_high = nameToJob_.upper_bound( name );

        for( auto it = it_low; it != it_high; ++it )
        {
            jobs.insert( it->second );
        }
    }

    int GetNumExec( int64_t jobId ) const
    {
        auto it = jobExecutions_.find( jobId );
        if ( it != jobExecutions_.end() )
        {
            return it->second;
        }
        return -1;
    }

    size_t GetNumJobs() const { return jobs_.left.size(); }

    JobIterator GetJobQueueBegin() const { return jobs_.right.begin(); }
    JobIterator GetJobQueueEnd() const { return jobs_.right.end(); }

    template< typename T >
    void SetOnRemoveCallback( T *obj, void (T::*f)( int64_t jobId, bool success ) )
    {
        onRemoveCallback_ = std::bind( f, obj, std::placeholders::_1, std::placeholders::_2 );
    }

    void RemoveJob( int64_t jobId, bool success, const char *completionStatus )
    {
        jobExecutions_.erase( jobId );

        if ( onRemoveCallback_ )
            onRemoveCallback_( jobId, success );

        auto it = jobs_.left.find( jobId );
        if ( it != jobs_.left.end() )
        {
            const JobState &jobState = it->second;
            const JobPtr &job = jobState.GetJob();
            RunJobCallback( job, completionStatus );
            ReleaseJob( job, success );
            jobs_.left.erase( it );
        }
        else
        {
            PLOG( "ScheduledJobs::RemoveJob: job not found for jobId=" << jobId );
        }
    }

    void Clear()
    {
        std::vector< int64_t > jobs;
        for( auto it = jobs_.left.begin(); it != jobs_.left.end(); ++it )
        {
            const int64_t jobId = it->first;
            jobs.push_back( jobId );
        }

        for( int64_t jobId : jobs )
        {
            RemoveJob( jobId, false, "timeout" );
        }
    }

private:
    void RunJobCallback( const JobPtr &job, const char *completionStatus )
    {
        std::ostringstream ss;
        ss << "Job completed, jobId=" << job->GetJobId() <<
            ", completion status: " << completionStatus;

        PLOG( ss.str() );

        boost::property_tree::ptree params;
        params.put( "job_id", job->GetJobId() );
        params.put( "status", completionStatus );

        job->RunCallback( "on_job_completion", params );
    }

    void ReleaseJob( const JobPtr &job, bool success )
    {
        if ( !job->GetName().empty() )
        {
            nameToJob_.erase( job->GetName() );
        }

        if ( job->GetJobGroup() )
        {
            const std::string &metaJobName = job->GetJobGroup()->GetName();
            ReleaseMetaJobName( metaJobName, job->GetJobId() );

            const bool lastJobInGroup = job->ReleaseJobGroup();
            if ( lastJobInGroup )
            {
                if ( success && job->GetJobGroup()->GetCron() )
                {
                    ICronManager *cronManager = common::GetService< ICronManager >();
                    cronManager->PushMetaJob( job->GetJobGroup() );
                }
                else
                {
                    IJobManager *jobManager = common::GetService< IJobManager >();
                    jobManager->ReleaseJobName( job->GetJobGroup()->GetName() );
                }
            }
        }
        else
        {
            if ( success && job->GetCron() )
            {
                ICronManager *cronManager = common::GetService< ICronManager >();
                cronManager->PushJob( job, true );
            }
            else
            {
                IJobManager *jobManager = common::GetService< IJobManager >();
                jobManager->ReleaseJobName( job->GetName() );
            }
        }
    }

    void ReleaseMetaJobName( const std::string &metaJobName, int64_t jobId )
    {
        auto it_low = nameToJob_.lower_bound( metaJobName );
        auto it_high = nameToJob_.upper_bound( metaJobName );

        for( auto it = it_low; it != it_high; ++it )
        {
            if ( jobId == it->second )
            {
                nameToJob_.erase( it );
                break;
            }
        }
    }

private:
    JobPriorityQueue jobs_;
    IdToJobExec jobExecutions_; // job_id -> num job remaining executions (== 0, if job execution completed)
    JobNameToJob nameToJob_;
    std::function< void (int64_t, bool) > onRemoveCallback_;
};


class JobExecHistory
{
    typedef std::map< std::string, int > IPToNumExec;
    struct JobHistory
    {
        IPToNumExec numExec_;
    };

    typedef std::map< int64_t, JobHistory > JobIdToHistory;
public:
    void IncrementNumExec( int64_t jobId, const std::string &hostIP )
    {
        JobHistory &jobHistory = history_[ jobId ];
        ++jobHistory.numExec_[ hostIP ];
    }

    void RemoveJob( int jobId )
    {
        history_.erase( jobId );
    }

    int GetNumExec( int64_t jobId, const std::string &hostIP ) const
    {
        const auto it = history_.find( jobId );
        if ( it != history_.end() )
        {
            const JobHistory &jobHistory = it->second;
            const IPToNumExec &numExec = jobHistory.numExec_;
            const auto e_it = numExec.find( hostIP );
            if ( e_it != numExec.end() )
                return e_it->second;
        }
        return 0;
    }

private:
    JobIdToHistory history_;
};

} // namespace master

#endif

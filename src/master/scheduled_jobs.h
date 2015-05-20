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

class ScheduledJobs
{
private:
    typedef std::map< int64_t, int > IdToJobExec;
    typedef std::multimap< std::string, int64_t > JobNameToJob;

public:
    typedef std::multiset< JobState > JobQueue;

public:
    void Add( JobPtr &job, int numExec )
    {
        jobExecutions_[ job->GetJobId() ] = numExec;

        if ( !job->GetName().empty() )
        {
            nameToJob_.insert( std::make_pair( job->GetName(), job->GetJobId() ) );
        }
        if ( job->GetJobGroup() )
        {
            const std::string &metaJobName = job->GetJobGroup()->GetName();
            if ( !metaJobName.empty() )
            {
                nameToJob_.insert( std::make_pair( metaJobName, job->GetJobId() ) );
            }
        }

        jobs_.insert( JobState( job ) );
    }

    void DecrementJobExecution( int64_t jobId, int numTasks )
    {
        auto it = jobExecutions_.find( jobId );
        if ( it != jobExecutions_.end() )
        {
            const int numExecution = it->second - numTasks;
            it->second = numExecution;
            if ( numExecution < 1 )
            {
                RemoveJob( jobId, true, "success" );
            }
        }
    }

    bool FindJobByJobId( int64_t jobId, JobPtr &job ) const
    {
        // TODO: use fixed-size array of rb-trees to handle both priorities & job lookup
        // or boost::bimap
        for( auto it = jobs_.cbegin(); it != jobs_.cend(); ++it )
        {
            const JobPtr &j = (*it).GetJob();
            if ( j->GetJobId() == jobId )
            {
                job = j;
                return true;
            }
        }
        return false;
    }

    void GetJobGroup( int64_t groupId, std::list< JobPtr > &jobs ) const
    {
        for( auto it = jobs_.cbegin(); it != jobs_.cend(); ++it )
        {
            const JobPtr &job = (*it).GetJob();
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

    size_t GetNumJobs() const { return jobs_.size(); }
    const JobQueue &GetJobQueue() const { return jobs_; }

    template< typename T >
    void SetOnRemoveCallback( T *obj, void (T::*f)( int64_t jobId, bool success ) )
    {
        onRemoveCallback_ = std::bind( f, obj, std::placeholders::_1, std::placeholders::_2 );
    }

    void RemoveJob( int64_t jobId, bool success, const char *completionStatus )
    {
        if ( onRemoveCallback_ )
            onRemoveCallback_( jobId, success );

        jobExecutions_.erase( jobId );
        for( auto it = jobs_.begin(); it != jobs_.end(); ++it )
        {
            const JobPtr &job = (*it).GetJob();
            if ( job->GetJobId() == jobId )
            {
                RunJobCallback( job, completionStatus );
                ReleaseJob( job, success );
                jobs_.erase( it );
                return;
            }
        }

        PLOG( "ScheduledJobs::RemoveJob: job not found for jobId=" << jobId );
    }

    void Clear()
    {
        JobQueue jobs( jobs_ );
        for( auto it = jobs_.cbegin(); it != jobs.cend(); ++it )
        {
            const JobPtr &job = (*it).GetJob();
            RemoveJob( job->GetJobId(), false, "timeout" );
        }
    }

private:
    void RunJobCallback( const JobPtr &job, const char *completionStatus )
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

    void ReleaseJob( const JobPtr &job, bool success )
    {
        if ( !job->GetName().empty() )
        {
            nameToJob_.erase( job->GetName() );
        }

        if ( job->GetJobGroup() )
        {
            const bool lastJobInGroup = job->ReleaseJobGroup();
            if ( lastJobInGroup )
            {
                const std::string &metaJobName = job->GetJobGroup()->GetName();
                if ( !metaJobName.empty() )
                {
                    nameToJob_.erase( metaJobName );
                }

                if ( success && job->GetCron() )
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

private:
    JobQueue jobs_;
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

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

#include "job.h"
#include "common/log.h"

namespace master {

JobGroup::JobGroup( IJobGroupEventReceiverPtr &evReceiver )
: eventReceiver_( evReceiver )
{}

void JobGroup::OnJobCompletion( const JobVertex &vertex )
{
    using namespace boost;

    JobGraph::out_edge_iterator i, i_end;
    for( tie( i, i_end ) = out_edges( vertex, graph_ ); i != i_end; ++i )
    {
        JobVertex out = target( *i, graph_ );
        JobPtr job = indexToJob_[ out ].lock();
        if ( job )
        {
            int numDeps = job->GetNumDepends();
            job->SetNumDepends( numDeps - 1 );
            if ( numDeps < 2 )
            {
                eventReceiver_->OnJobDependenciesResolved( job );
            }
        }
    }
}

void Job::ReleaseJobGroup()
{
    if ( jobGroup_ )
        jobGroup_->OnJobCompletion( graphVertex_ );
}

bool Job::IsHostPermitted( const std::string &host ) const
{
    if ( !hosts_.size() )
        return true;

    std::set< std::string >::const_iterator it = hosts_.find( host );
    return it != hosts_.end();
}

bool Job::IsGroupPermitted( const std::string &group ) const
{
    if ( !groups_.size() )
        return true;

    std::set< std::string >::const_iterator it = groups_.find( group );
    return it != groups_.end();
}


void JobQueueImpl::PushJob( JobPtr &job, int64_t groupId )
{
    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
    job->SetGroupId( groupId );
    idToJob_[ job->GetJobId() ] = job;
    jobs_.push_back( job );
    std::push_heap( jobs_.begin(), jobs_.end(), JobComparatorPriority() );
}

void JobQueueImpl::PushJobs( std::list< JobPtr > &jobs, int64_t groupId )
{
    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
    std::list< JobPtr >::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        const JobPtr &job = *it;
        job->SetGroupId( groupId );
        idToJob_[ job->GetJobId() ] = job;
        if ( job->GetNumDepends() )
        {
            delayedJobs_.insert( job );
        }
        else
        {
            jobs_.push_back( job );
            std::push_heap( jobs_.begin(), jobs_.end(), JobComparatorPriority() );
        }
    }
}

bool JobQueueImpl::GetJobById( int64_t jobId, JobPtr &job )
{
    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
    IdToJob::const_iterator it = idToJob_.find( jobId );
    if ( it != idToJob_.end() )
    {
        job = it->second;
        return true;
    }
    return false;
}

bool JobQueueImpl::DeleteJob( int64_t jobId )
{
    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );

    IdToJob::iterator it = idToJob_.find( jobId );
    if ( it == idToJob_.end() )
        return false;
    JobPtr job( it->second );
    idToJob_.erase( it );

    OnJobDeletion( job );

    {
        JobList::iterator it = jobs_.begin();
        for( ; it != jobs_.end(); ++it )
        {
            const JobPtr &job = *it;
            if ( job->GetJobId() == jobId )
            {
                jobs_.erase( it );
                std::make_heap( jobs_.begin(), jobs_.end(), JobComparatorPriority() );
                return true;
            }
        }
    }

    delayedJobs_.erase( job );
    return true;
}

void JobQueueImpl::OnJobDeletion( JobPtr &job ) const
{
    std::ostringstream ss;
    ss << "================" << std::endl <<
        "Job deleted from job queue, jobId = " << job->GetJobId() << std::endl <<
        "completion status: failed" << std::endl <<
        "================";

    PLOG( ss.str() );

    boost::property_tree::ptree params;
    params.put( "job_id", job->GetJobId() );
    params.put( "user_msg", ss.str() );

    job->RunCallback( "on_job_deletion", params );
    job->ReleaseJobGroup();
}

bool JobQueueImpl::DeleteJobGroup( int64_t groupId )
{
    JobList jobs;
    bool deleted = false;
    {
        std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
        JobList::const_iterator it = jobs_.begin();
        for( ; it != jobs_.end(); ++it )
        {
            const JobPtr &job = *it;
            if ( job->GetGroupId() == groupId )
                jobs.push_back( job );
        }
        JobSet::const_iterator its = delayedJobs_.begin();
        for( ; its != delayedJobs_.end(); ++its )
        {
            const JobPtr &job = *its;
            if ( job->GetGroupId() == groupId )
                jobs.push_back( job );
        }
    }

    JobList::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        const JobPtr &job = *it;
        if ( job->GetGroupId() == groupId )
            deleted = DeleteJob( job->GetJobId() );
    }

    return deleted;
}

void JobQueueImpl::Clear()
{
    JobList jobs;
    {
        std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
        jobs = jobs_;
        jobs.insert( jobs.end(), delayedJobs_.begin(), delayedJobs_.end() );
        // std::copy( delayedJobs.begin(), delayedJobs.end(), std::back_inserter( jobs ) ); // less effective
    }

    JobList::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        const JobPtr &job = *it;
        DeleteJob( job->GetJobId() );
    }
}

bool JobQueueImpl::PopJob( JobPtr &job )
{
    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
    if ( !jobs_.empty() )
    {
        job = jobs_.front();
        std::pop_heap( jobs_.begin(), jobs_.end(), JobComparatorPriority() );
        jobs_.pop_back();
        idToJob_.erase( job->GetJobId() );
        return true;
    }
    return false;
}

void JobQueueImpl::OnJobDependenciesResolved( const JobPtr &job )
{
    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
    JobSet::iterator it = delayedJobs_.find( job );
    if ( it != delayedJobs_.end() )
    {
        jobs_.push_back( job );
        std::push_heap( jobs_.begin(), jobs_.end(), JobComparatorPriority() );
        delayedJobs_.erase( it );
    }
    else
    {
        PLOG_WRN( "JobQueueImpl::OnJobDependenciesResolved: unknown delayed job, jobId=" << job->GetJobId() );
    }
}

} // namespace master

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
#include "job_manager.h"
#include "common/log.h"
#include "common/service_locator.h"

namespace master {

JobGroup::JobGroup( IJobGroupEventReceiverPtr &evReceiver )
: eventReceiver_( evReceiver ),
 numCompleted_( 0 )
{}

bool JobGroup::OnJobCompletion( const JobVertex &vertex )
{
    using namespace boost;

    bool lastJobInGroup = false;

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
            ++numCompleted_;
            lastJobInGroup = (numCompleted_ == indexToJob_.size());
        }
    }

    return lastJobInGroup;
}

bool Job::ReleaseJobGroup()
{
    return jobGroup_ && jobGroup_->OnJobCompletion( graphVertex_ );
}

void Job::AddHost( const std::string &host )
{
    if ( blacklistHosts_.find( host ) == blacklistHosts_.end() )
    {
        hosts_.insert( host );
    }
    else
    {
        PLOG_WRN( "Job::AddHost: host already in black list: host=" << host );
    }
}

void Job::AddHostToBlacklist( const std::string &host )
{
    // blacklist has more priority than white list
    blacklistHosts_.insert( host );
    auto it = hosts_.find( host );
    if ( it != hosts_.end() )
    {
        hosts_.erase( it );
        PLOG_WRN( "Job::AddHostToBlacklist: removing host from 'hosts' list: host=" << host );
    }
}

bool Job::IsHostPermitted( const std::string &host ) const
{
    if ( blacklistHosts_.find( host ) != blacklistHosts_.end() )
        return false;

    if ( !hosts_.size() )
        return true;

    return hosts_.find( host ) != hosts_.end();
}

size_t Job::GetNumPermittedHosts() const
{
    return hosts_.size();
}

void Job::AddGroup( const std::string &group )
{
    if ( blacklistGroups_.find( group ) == blacklistGroups_.end() )
    {
        groups_.insert( group );
    }
    else
    {
        PLOG_WRN( "Job::AddGroup: group already in black list: group=" << group );
    }
}

void Job::AddGroupToBlacklist( const std::string &group )
{
    // blacklist has more priority than white list
    blacklistGroups_.insert( group );
    auto it = groups_.find( group );
    if ( it != groups_.end() )
    {
        groups_.erase( it );
        PLOG_WRN( "Job::AddGroupToBlacklist: removing group from 'groups' list: group=" << group );
    }
}

bool Job::IsGroupPermitted( const std::string &group ) const
{
    if ( blacklistGroups_.find( group ) != blacklistGroups_.end() )
        return false;

    if ( !groups_.size() )
        return true;

    return groups_.find( group ) != groups_.end();
}


void JobQueue::PushJob( JobPtr &job, int64_t groupId )
{
    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
    job->SetGroupId( groupId );
    idToJob_[ job->GetJobId() ] = job;

    if ( !job->GetName().empty() )
        nameToJob_.insert( std::make_pair( job->GetName(), job ) );

    jobs_.push_back( job );
    std::push_heap( jobs_.begin(), jobs_.end(), JobComparatorPriority() );
}

void JobQueue::PushJobs( std::list< JobPtr > &jobs, int64_t groupId )
{
    std::string metaJobName;
    const auto jobGroup = jobs.front()->GetJobGroup();
    if ( jobGroup )
        metaJobName = jobGroup->GetName();

    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
    for( const auto &job : jobs )
    {
        job->SetGroupId( groupId );
        idToJob_[ job->GetJobId() ] = job;

        if ( !metaJobName.empty() )
            nameToJob_.insert( std::make_pair( metaJobName, job ) );
        if ( !job->GetName().empty() )
            nameToJob_.insert( std::make_pair( job->GetName(), job ) );

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

bool JobQueue::GetJobById( int64_t jobId, JobPtr &job )
{
    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
    auto it = idToJob_.find( jobId );
    if ( it != idToJob_.end() )
    {
        job = it->second;
        return true;
    }
    return false;
}

bool JobQueue::DeleteJob( int64_t jobId )
{
    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );

    auto it = idToJob_.find( jobId );
    if ( it == idToJob_.end() )
        return false;
    JobPtr job( it->second );
    idToJob_.erase( it );

    if ( !job->GetName().empty() )
    {
        nameToJob_.erase( job->GetName() );
        IJobManager *jobManager = common::GetService< IJobManager >();
        jobManager->ReleaseJobName( job->GetName() );
    }

    OnJobDeletion( job );

    for( auto it = jobs_.begin(); it != jobs_.end(); ++it )
    {
        const JobPtr &job = *it;
        if ( job->GetJobId() == jobId )
        {
            jobs_.erase( it );
            std::make_heap( jobs_.begin(), jobs_.end(), JobComparatorPriority() );
            return true;
        }
    }

    delayedJobs_.erase( job );
    return true;
}

void JobQueue::GetJobsByName( const std::string &name, std::set< JobPtr > &jobs )
{
    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );

    auto it_low = nameToJob_.lower_bound( name );
    auto it_high = nameToJob_.upper_bound( name );

    for( auto it = it_low; it != it_high; ++it )
    {
        jobs.insert( it->second );
    }
}

void JobQueue::OnJobDeletion( JobPtr &job )
{
    std::ostringstream ss;
    ss << "Job deleted from job queue, jobId=" << job->GetJobId() <<
        ", completion status: failed";

    PLOG( ss.str() );

    boost::property_tree::ptree params;
    params.put( "job_id", job->GetJobId() );
    params.put( "user_msg", ss.str() );

    job->RunCallback( "on_job_deletion", params );

    if ( job->GetJobGroup() )
    {
        const std::string &metaJobName = job->GetJobGroup()->GetName();
        ReleaseMetaJobName( metaJobName, job->GetJobId() );

        const bool lastJobInGroup = job->ReleaseJobGroup();
        if ( lastJobInGroup )
        {
            IJobManager *jobManager = common::GetService< IJobManager >();
            jobManager->ReleaseJobName( metaJobName );
        }
    }
}

bool JobQueue::DeleteJobGroup( int64_t groupId )
{
    JobList jobs;
    {
        std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
        for( const auto &job : jobs_ )
        {
            if ( job->GetGroupId() == groupId )
                jobs.push_back( job );
        }
        for( const auto &job : delayedJobs_ )
        {
            if ( job->GetGroupId() == groupId )
                jobs.push_back( job );
        }
    }

    bool deleted = false;
    for( const auto &job : jobs )
    {
        if ( job->GetGroupId() == groupId )
            deleted = DeleteJob( job->GetJobId() );
    }

    return deleted;
}

void JobQueue::ReleaseMetaJobName( const std::string &metaJobName, int64_t jobId )
{
    auto it_low = nameToJob_.lower_bound( metaJobName );
    auto it_high = nameToJob_.upper_bound( metaJobName );

    for( auto it = it_low; it != it_high; ++it )
    {
        const auto &job = it->second;
        if ( jobId == job->GetJobId() )
        {
            nameToJob_.erase( it );
            break;
        }
    }
}

void JobQueue::Clear()
{
    JobList jobs;
    {
        std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
        jobs = jobs_;
        jobs.insert( jobs.end(), delayedJobs_.begin(), delayedJobs_.end() );
        //std::copy( delayedJobs.begin(), delayedJobs.end(), std::back_inserter( jobs ) ); // less effective
    }

    for( const auto &job : jobs )
    {
        DeleteJob( job->GetJobId() );
    }
}

bool JobQueue::PopJob( JobPtr &job )
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

void JobQueue::OnJobDependenciesResolved( const JobPtr &job )
{
    std::unique_lock< std::recursive_mutex > lock( jobsMut_ );
    auto it = delayedJobs_.find( job );
    if ( it != delayedJobs_.end() )
    {
        jobs_.push_back( job );
        std::push_heap( jobs_.begin(), jobs_.end(), JobComparatorPriority() );
        delayedJobs_.erase( it );
    }
    else
    {
        PLOG_WRN( "JobQueue::OnJobDependenciesResolved: unknown delayed job, jobId=" << job->GetJobId() );
    }
}

} // namespace master

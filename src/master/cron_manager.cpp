/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2015 Andrey Budnik <budnik27@gmail.com>

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

#include "cron_manager.h"
#include "job_manager.h"
#include "common/service_locator.h"

namespace master {

CronManager::TimeoutHandler::TimeoutHandler()
: removed_( false )
{}

void CronManager::JobTimeoutHandler::HandleTimeout()
{
    IJobManager *jobManager = common::GetService< IJobManager >();
    jobManager->BuildAndPushJob( -1, jobDescription_ );
}

void CronManager::MetaJobTimeoutHandler::HandleTimeout()
{
    IJobManager *jobManager = common::GetService< IJobManager >();
    jobManager->BuildAndPushJob( -1, jobDescription_ );
}


void CronManager::Start()
{
    io_service_.post( boost::bind( &CronManager::Run, this ) );
}

void CronManager::Stop()
{
    stopped_ = true;
    timer_.StopWaiting();
}

void CronManager::Run()
{
    while( !stopped_ )
    {
        timer_.Wait( 1000 );
        CheckTimeouts();
    }
}

void CronManager::CheckTimeouts()
{
    std::unique_lock< std::mutex > lock( jobsMut_ );
    auto it = jobs_.begin();
    const auto now = std::chrono::system_clock::now();
    for( ; it != jobs_.end(); )
    {
        const ptime &jobPlannedTime = it->first;
        if ( now < jobPlannedTime ) // skip earlier planned jobs
            break;

        CallbackPtr &callback = it->second;
        if ( !callback->removed_ )
        {
            callback->HandleTimeout();
            callbacks_.erase( callback->jobName_ );
        }
        jobs_.erase( it++ );
    }
}

void CronManager::PushJob( const JobPtr &job, bool afterExecution )
{
    const auto now = std::chrono::system_clock::now();
    auto deadline = job->GetCron().Next( now );
    if ( afterExecution )
    {
        if ( deadline <= now )
        {
            deadline += std::chrono::minutes( 1 );
        }
    }
    else
    {
        IJobManager *jobManager = common::GetService< IJobManager >();
        jobManager->RegisterJobName( job->GetName() );
    }

    auto handler = std::make_shared< JobTimeoutHandler >();
    handler->jobDescription_ = job->GetDescription();
    handler->jobName_ = job->GetName();

    std::unique_lock< std::mutex > lock( jobsMut_ );
    jobs_.insert( std::pair< ptime, CallbackPtr >(
                      deadline,
                      handler
                )
    );

    callbacks_[ job->GetName() ] = handler;
}

void CronManager::PushMetaJob( const JobGroupPtr &metaJob )
{
    const auto now = std::chrono::system_clock::now();
    auto deadline = metaJob->GetCron().Next( now );
    if ( deadline <= now )
    {
        deadline += std::chrono::minutes( 1 );
    }

    auto handler = std::make_shared< MetaJobTimeoutHandler >();
    handler->jobDescription_ = metaJob->GetDescription();
    handler->jobName_ = metaJob->GetName();

    std::unique_lock< std::mutex > lock( jobsMut_ );
    jobs_.insert( std::pair< ptime, CallbackPtr >(
                      deadline,
                      handler
                )
    );

    callbacks_[ metaJob->GetName() ] = handler;
}

void CronManager::PushMetaJob( std::list< JobPtr > &jobs )
{
    IJobManager *jobManager = common::GetService< IJobManager >();
    auto metaJob = jobs.front()->GetJobGroup();

    const auto now = std::chrono::system_clock::now();
    auto deadline = metaJob->GetCron().Next( now );

    jobManager->RegisterJobName( metaJob->GetName() );

    auto handler = std::make_shared< MetaJobTimeoutHandler >();
    handler->jobDescription_ = metaJob->GetDescription();
    handler->jobName_ = metaJob->GetName();

    for( const auto &job : jobs )
    {
        if ( !job->GetName().empty() )
        {
            handler->jobNames.insert( job->GetName() );
            jobManager->RegisterJobName( job->GetName() );
        }
    }

    std::unique_lock< std::mutex > lock( jobsMut_ );
    jobs_.insert( std::pair< ptime, CallbackPtr >(
                      deadline,
                      handler
                )
    );

    callbacks_[ metaJob->GetName() ] = handler;
}

void CronManager::StopJob( const std::string &jobName )
{
    std::unique_lock< std::mutex > lock( jobsMut_ );
    auto it = callbacks_.find( jobName );
    if ( it != callbacks_.end() )
    {
        IJobManager *jobManager = common::GetService< IJobManager >();
        const CallbackPtr &handler = it->second;
        handler->removed_ = true;

        auto handler_meta = std::dynamic_pointer_cast< MetaJobTimeoutHandler >( handler );
        if ( handler_meta )
        {
            auto it_j = handler_meta->jobNames.begin();
            for( ; it_j != handler_meta->jobNames.end(); ++it )
            {
                const std::string &jobName = *it_j;
                jobManager->ReleaseJobName( jobName );
            }
        }

        jobManager->ReleaseJobName( jobName );
        callbacks_.erase( it );
    }
}

} // namespace master

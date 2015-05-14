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

void CronManager::JobTimeoutHandler::HandleTimeout()
{
    IJobManager *jobManager = common::GetService< IJobManager >();
    jobManager->PushJobFromHistory( -1, jobDescription_ );
}

void CronManager::MetaJobTimeoutHandler::HandleTimeout()
{
    IJobManager *jobManager = common::GetService< IJobManager >();
    jobManager->PushJobFromHistory( -1, metaJobDescription_ );
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

        Callback callback( it->second );
        callback();
        jobs_.erase( it++ );
    }
}

void CronManager::PushJob( const JobPtr &job, bool afterExecution )
{
    const auto now = std::chrono::system_clock::now();
    auto deadline = job->GetCron().Next( now );
    if ( deadline <= now && afterExecution )
    {
        deadline += std::chrono::minutes( 1 );
    }

    auto handler = std::make_shared< JobTimeoutHandler >();
    handler->jobDescription_ = job->GetDescription();
    Callback callback(
        std::bind( &JobTimeoutHandler::HandleTimeout, handler )
    );

    std::unique_lock< std::mutex > lock( jobsMut_ );
    jobs_.insert( std::pair< ptime, Callback >(
                      deadline,
                      callback
                )
    );
}

void CronManager::PushMetaJob( const JobGroupPtr &metaJob, bool afterExecution )
{
    const auto now = std::chrono::system_clock::now();
    auto deadline = metaJob->GetCron().Next( now );
    if (  deadline <= now && afterExecution )
    {
        deadline += std::chrono::minutes( 1 );
    }

    auto handler = std::make_shared< MetaJobTimeoutHandler >();
    handler->metaJobDescription_ = metaJob->GetDescription();
    Callback callback(
        std::bind( &MetaJobTimeoutHandler::HandleTimeout, handler )
    );

    std::unique_lock< std::mutex > lock( jobsMut_ );
    jobs_.insert( std::pair< ptime, Callback >(
                      deadline,
                      callback
                )
    );
}

} // namespace master

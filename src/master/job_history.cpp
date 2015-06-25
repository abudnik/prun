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

#include <boost/lexical_cast.hpp>
#include "job_history.h"
#include "job_manager.h"
#include "common/service_locator.h"


namespace master {

JobHistory::JobHistory( common::IHistory *history )
: history_( history )
{}

void JobHistory::OnJobAdd( const std::string &jobId, const std::string &jobDescr )
{
    if ( history_ )
    {
        try
        {
            history_->Put( jobId, jobDescr );
        }
        catch( const std::exception &e )
        {
            PLOG_ERR( "JobHistory::OnJobAdd: " << e.what() );
        }
    }
}

void JobHistory::OnJobDelete( int64_t jobId, const std::string &jobName )
{
    if ( history_ )
    {
        try
        {
            if ( jobId >= 0 )
                history_->Delete( std::to_string( jobId ) );

            if ( !jobName.empty() )
                history_->Delete( jobName );
        }
        catch( const std::exception &e )
        {
            PLOG_ERR( "JobHistory::OnJobDelete: " << e.what() );
        }
    }
}

void OnGetJobCompleted( const std::string &key, const std::string &value )
{
    //PLOG( "OnGetJobCompleted : key=" << key << ", value=" << value );
    int64_t id;
    try
    {
        id = boost::lexical_cast<int64_t>( key );
    }
    catch( ... )
    {
        id = -1;
    }
    IJobManager *jobManager = common::GetService< IJobManager >();
    jobManager->BuildAndPushJob( id, value );
}

void JobHistory::GetJobs()
{
    if ( history_ )
    {
        try
        {
            history_->GetAll( OnGetJobCompleted );
        }
        catch( const std::exception &e )
        {
            PLOG_ERR( "JobHistory::GetJobs: " << e.what() );
        }
    }
}

} // namespace master

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

JobHistory::JobHistory( IHistoryChannel *channel )
: channel_( channel )
{
    addCallback_ = std::bind( &JobHistory::OnAddCompleted, this, std::placeholders::_1 );
    deleteCallback_ = std::bind( &JobHistory::OnDeleteCompleted, this, std::placeholders::_1 );
    getCallback_ = std::bind( &JobHistory::OnGetCompleted, this, std::placeholders::_1 );
}

void JobHistory::OnJobAdd( const JobPtr &job )
{
    std::string request( "PUT " );
    request += std::to_string( job->GetJobId() );
    request += '$';
    request += job->GetDescription();
    request.insert( 0, std::to_string( request.size() ) + '\n' );

    channel_->Send( request, addCallback_ );
}

void JobHistory::OnAddCompleted( const std::string &response )
{
    //PLOG( "OnAddCompleted : " << response );
}

void JobHistory::OnJobDelete( int64_t jobId )
{
    std::string request( "DELETE " );
    request += std::to_string( jobId );
    request += '$';
    request.insert( 0, std::to_string( request.size() ) + '\n' );

    channel_->Send( request, deleteCallback_ );
}

void JobHistory::OnDeleteCompleted( const std::string &response )
{
    //PLOG( "OnDeleteCompleted : " << response );
}

void JobHistory::GetJobs()
{
    std::string request( "GET $" );
    request.insert( 0, std::to_string( request.size() ) + '\n' );

    channel_->Send( request, getCallback_ );
}

void JobHistory::OnGetCompleted( const std::string &response )
{
    //PLOG( "OnGetCompleted : " << response );
    size_t offset = 1; // skip newline after header
    unsigned line = 0;
    std::string jobId, jobDescription;
    while( true )
    {
        size_t pos = response.find( '\n', offset );
        if ( pos == std::string::npos )
            break;

        if ( line % 2 )
        {
            jobDescription = std::string( response, offset, pos - offset );
            //PLOG( "jobId = " << jobId );
            //PLOG( "jobDescr = " << jobDescription );

            int64_t id = boost::lexical_cast<int64_t>( jobId );
            IJobManager *jobManager = common::GetService< IJobManager >();
            jobManager->PushJobFromHistory( id, jobDescription );
        }
        else
        {
            jobId = std::string( response, offset, pos - offset );
        }

        offset = pos + 1;
        ++line;
    }

    PLOG( "JobHistory::OnGetCompleted: " << line / 2 << " jobs are loaded from history" );
}

} // namespace master

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

#include <sstream>
#include "user_command.h"
#include "job_manager.h"
#include "worker_manager.h"
#include "scheduler.h"
#include "common/log.h"

namespace master {

bool UserCommand::Run( const std::string &filePath, const std::string &jobAlias, std::string &result )
{
    try
    {
        // read job description from file
        std::ifstream file( filePath.c_str() );
        if ( !file.is_open() )
        {
            PLOG_ERR( "UserCommand::Run: couldn't open '" << filePath << "'" );
            return false;
        }

        size_t found = filePath.rfind( '.' );
        if ( found == std::string::npos )
        {
            PLOG_ERR( "UserCommand::Run: couldn't extract job file extension '" << filePath << "'" );
            return false;
        }
        std::string ext = filePath.substr( found + 1 );

        if ( ext == "job" )
            return RunJob( file, jobAlias, result );
        else
        if ( ext == "meta" )
            return RunMetaJob( file, result );
        else
            PLOG_ERR( "UserCommand::Run: unknown file extension '" << ext << "'" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::Run: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::RunJob( std::ifstream &file, const std::string &jobAlias, std::string &result ) const
{
    try
    {
        std::string jobDescr, line;
        while( std::getline( file, line ) )
            jobDescr += line;

        Job *job = JobManager::Instance().CreateJob( jobDescr );
        if ( job )
        {
            job->SetAlias( jobAlias );
            PrintJobInfo( job, result );
            // add job to job queue
            JobManager::Instance().PushJob( job );
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::RunJob: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::RunMetaJob( std::ifstream &file, std::string &result ) const
{
    try
    {
        std::string metaDescr, line;
        while( getline( file, line ) )
            metaDescr += line + '\n';

        std::list< JobPtr > jobs;
        JobManager::Instance().CreateMetaJob( metaDescr, jobs );
        JobManager::Instance().PushJobs( jobs );

        std::ostringstream ss;
        ss << "----------------" << std::endl;
        std::list< JobPtr >::const_iterator it = jobs.begin();
        for( ; it != jobs.end(); ++it )
        {
            PrintJobInfo( (*it).get(), result );
            ss << result << std::endl;
        }
        ss << "----------------" << std::endl;
        result = ss.str();
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::RunMetaJob: " << e.what() );
        return false;
    }
    return true;
}

void UserCommand::PrintJobInfo( const Job *job, std::string &result ) const
{
    std::ostringstream ss;
    ss << "jobId = " << job->GetJobId() << ", groupId = " << job->GetGroupId();
    result = ss.str();
}

bool UserCommand::Stop( int64_t jobId )
{
    try
    {
        if ( !JobManager::Instance().DeleteJob( jobId ) )
        {
            Scheduler::Instance().StopJob( jobId );
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::Stop: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::StopGroup( int64_t groupId )
{
    try
    {
        JobManager::Instance().DeleteJobGroup( groupId );
        Scheduler::Instance().StopJobGroup( groupId );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::StopGroup: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::StopAll()
{
    try
    {
        JobManager::Instance().DeleteAllJobs();
        Scheduler::Instance().StopAllJobs();
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::StopAll: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::StopPreviousJobs()
{
    try
    {
        Scheduler::Instance().StopPreviousJobs();
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::StopPreviousJobs: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::AddWorkerHost( const std::string &groupName, const std::string &host )
{
    try
    {
        WorkerManager &mgr = WorkerManager::Instance();
        mgr.AddWorkerHost( groupName, host );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::AddWorkerHost: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::DeleteHost( const std::string &host )
{
    try
    {
        WorkerManager &mgr = WorkerManager::Instance();
        mgr.DeleteWorkerHost( host );

        Scheduler &scheduler = Scheduler::Instance();
        scheduler.DeleteWorker( host );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::DeleteHost: " << e.what() );
        return false;
    }
    return true;
}


bool UserCommand::AddGroup( const std::string &filePath, const std::string &fileName )
{
    try
    {
        std::list< std::string > hosts;
        if ( ReadHosts( filePath.c_str(), hosts ) )
        {
            WorkerManager::Instance().AddWorkerGroup( fileName, hosts );
        }
        else
            return false;
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::AddGroup: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::DeleteGroup( const std::string &group )
{
    try
    {
        std::vector< WorkerPtr > workers;
        WorkerManager::Instance().GetWorkers( workers, group );
        WorkerManager::Instance().DeleteWorkerGroup( group );

        Scheduler &scheduler = Scheduler::Instance();
        std::vector< WorkerPtr >::const_iterator it = workers.begin();
        for( ; it != workers.end(); ++it )
        {
            const WorkerPtr &w = *it;
            scheduler.DeleteWorker( w->GetHost() );
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::DeleteGroup: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::Info( int64_t jobId, std::string &result )
{
    try
    {
        Scheduler::Instance().GetJobInfo( result, jobId );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::Info: " << e.what() );
        return false;
    }
    return true;
}


bool UserCommand::GetStatistics( std::string &result )
{
    try
    {
        Scheduler::Instance().GetStatistics( result );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::GetStatistics: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::GetAllJobInfo( std::string &result )
{
    try
    {
        Scheduler::Instance().GetAllJobInfo( result );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::GetAllJobInfo: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::GetWorkersStatistics( std::string &result )
{
    try
    {
        Scheduler::Instance().GetWorkersStatistics( result );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::GetWorkersStatistics: " << e.what() );
        return false;
    }
    return true;
}

} // namespace master

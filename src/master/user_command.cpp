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
#include "cron_manager.h"
#include "scheduler.h"
#include "statistics.h"
#include "common/log.h"
#include "common/service_locator.h"

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

        IJobManager *jobManager = common::GetService< IJobManager >();
        JobPtr job( jobManager->CreateJob( jobDescr, true ) );
        if ( job )
        {
            job->SetAlias( jobAlias );

            if ( job->GetCron() )
            {
                ICronManager *cronManager = common::GetService< ICronManager >();
                cronManager->PushJob( job, false );
            }
            else
            {
                // add job to job queue
                jobManager->PushJob( job );
                PrintJobInfo( job, result );
            }
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
            metaDescr += line;

        std::list< JobPtr > jobs;
        IJobManager *jobManager = common::GetService< IJobManager >();
        if ( !jobManager->CreateMetaJob( metaDescr, jobs, true ) )
            return false;

        if ( !jobs.empty() )
        {
            auto jobGroup = jobs.front()->GetJobGroup();
            if ( jobGroup->GetCron() )
            {
                ICronManager *cronManager = common::GetService< ICronManager >();
                cronManager->PushMetaJob( jobs );
            }
            else
            {
                jobManager->PushJobs( jobs );

                std::ostringstream ss;
                ss << "----------------" << std::endl;
                for( const auto &job : jobs )
                {
                    PrintJobInfo( job, result );
                    ss << result << std::endl;
                }
                ss << "----------------" << std::endl;
                result = ss.str();
            }
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::RunMetaJob: " << e.what() );
        return false;
    }
    return true;
}

void UserCommand::PrintJobInfo( const JobPtr &job, std::string &result ) const
{
    std::ostringstream ss;
    ss << "jobId = " << job->GetJobId() << ", groupId = " << job->GetGroupId();
    result = ss.str();
}

bool UserCommand::Stop( int64_t jobId )
{
    try
    {
        IJobManager *jobManager = common::GetService< IJobManager >();
        if ( !jobManager->DeleteJob( jobId ) )
        {
            IScheduler *scheduler = common::GetService< IScheduler >();
            scheduler->StopJob( jobId );
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
        IJobManager *jobManager = common::GetService< IJobManager >();
        jobManager->DeleteJobGroup( groupId );
        IScheduler *scheduler = common::GetService< IScheduler >();
        scheduler->StopJobGroup( groupId );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::StopGroup: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::StopNamed( const std::string &name )
{
    try
    {
        ICronManager *cronManager = common::GetService< ICronManager >();
        cronManager->StopJob( name );
        IJobManager *jobManager = common::GetService< IJobManager >();
        jobManager->DeleteNamedJob( name );
        IScheduler *scheduler = common::GetService< IScheduler >();
        scheduler->StopNamedJob( name );

    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::StopNamed: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::StopAll()
{
    try
    {
        ICronManager *cronManager = common::GetService< ICronManager >();
        cronManager->StopAllJobs();
        IJobManager *jobManager = common::GetService< IJobManager >();
        jobManager->DeleteAllJobs();
        IScheduler *scheduler = common::GetService< IScheduler >();
        scheduler->StopAllJobs();
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
        IScheduler *scheduler = common::GetService< IScheduler >();
        scheduler->StopPreviousJobs();
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
        IWorkerManager *workerManager = common::GetService< IWorkerManager >();
        workerManager->AddWorkerHost( groupName, host );
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
        IWorkerManager *workerManager = common::GetService< IWorkerManager >();
        workerManager->DeleteWorkerHost( host );

        IScheduler *scheduler = common::GetService< IScheduler >();
        scheduler->DeleteWorker( host );
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
            IWorkerManager *workerManager = common::GetService< IWorkerManager >();
            workerManager->AddWorkerGroup( fileName, hosts );
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
        IWorkerManager *workerManager = common::GetService< IWorkerManager >();
        workerManager->GetWorkers( workers, group );
        workerManager->DeleteWorkerGroup( group );

        IScheduler *scheduler = common::GetService< IScheduler >();
        for( const auto &w : workers )
        {
            scheduler->DeleteWorker( w->GetHost() );
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
        JobInfo jobInfo( jobId );
        IScheduler *scheduler = common::GetService< IScheduler >();
        scheduler->Accept( &jobInfo );
        jobInfo.GetInfo( result );
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
        Statistics stat;
        IScheduler *scheduler = common::GetService< IScheduler >();
        scheduler->Accept( &stat );
        stat.GetInfo( result );
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
        AllJobInfo jobInfo;
        IScheduler *scheduler = common::GetService< IScheduler >();
        scheduler->Accept( &jobInfo );
        jobInfo.GetInfo( result );
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
        WorkerStatistics stat;
        IScheduler *scheduler = common::GetService< IScheduler >();
        scheduler->Accept( &stat );
        stat.GetInfo( result );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::GetWorkersStatistics: " << e.what() );
        return false;
    }
    return true;
}

bool UserCommand::GetCronInfo( std::string &result )
{
    try
    {
        CronStatistics stat;
        ICronManager *cronManager = common::GetService< ICronManager >();
        cronManager->Accept( &stat );
        stat.GetInfo( result );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "UserCommand::GetCronInfo: " << e.what() );
        return false;
    }
    return true;
}

} // namespace master

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

#define BOOST_SPIRIT_THREADSAFE

#include <boost/property_tree/json_parser.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/graph/depth_first_search.hpp>
#include <boost/graph/visitors.hpp>
#include <iterator>
#include "job_manager.h"
#include "job_history.h"
#include "common/log.h"
#include "common/config.h"
#include "common/helper.h"
#include "common/service_locator.h"
#include "scheduler.h"
#include "timeout_manager.h"

namespace boost {

struct cycle_detector : public dfs_visitor<>
{
    cycle_detector( bool &has_cycle )
    : has_cycle_( has_cycle ) {}

    template< class Edge, class Graph >
    void back_edge( Edge, Graph & ) { has_cycle_ = true; }
private:
    bool &has_cycle_;
};

} // namespace boost

namespace master {

bool JDLJson::ParseJob( const std::string &job_description, boost::property_tree::ptree &ptree )
{
    std::istringstream ss( job_description );
    try
    {
        boost::property_tree::read_json( ss, ptree );
    }
    catch( boost::property_tree::json_parser::json_parser_error &e )
    {
        PLOG_ERR( "JDLJson::ParseJob read_json failed: " << e.what() );
        return false;
    }
    return true;
}

JobManager::JobManager()
: jobs_( new JobQueue ),
 timeoutManager_( nullptr ),
 numJobGroups_( 0 ),
 jobId_( 0 )
{}

Job *JobManager::CreateJob( const std::string &job_description, bool check_name_existance )
{
    boost::property_tree::ptree ptree;
    JDLJson parser;
    if ( !parser.ParseJob( job_description, ptree ) )
        return nullptr;

    Job *job = CreateJob( ptree );
    if ( job )
    {
        job->SetDescription( job_description );

        if ( check_name_existance && HasJobName( job->GetName() ) )
        {
            PLOG_ERR( "JobManager::CreateJob: job name already exists: " << job->GetName() );
            delete job;
            return nullptr;
        }
    }
    return job;
}

bool JobManager::CreateMetaJob( const std::string &meta_description, std::list< JobPtr > &jobs, bool check_name_existance )
{
    boost::property_tree::ptree ptree;
    JDLJson parser;
    if ( !parser.ParseJob( meta_description, ptree ) )
        return false;

    try
    {
        typedef std::set< std::string > StringSet;
        StringSet jobFiles;

        // read job description file pathes
        for( const auto &adjList : ptree.get_child( "graph" ) )
        {
            for( const auto &item : adjList.second )
            {
                std::string job = item.second.get_value< std::string >();
                jobFiles.insert( job );
            }
        }

        std::map< std::string, int > jobFileToIndex;

        IJobGroupEventReceiverPtr evReceiverPtr = static_cast< IJobGroupEventReceiver * >( this );
        auto jobGroup = std::make_shared< JobGroup >( evReceiverPtr );
        std::vector< JobWeakPtr > &indexToJob = jobGroup->GetIndexToJob();

        if ( ptree.count( "cron" ) > 0 )
        {
            std::string cron_description = ptree.get<std::string>( "cron" );
            if ( !jobGroup->GetCron().Parse( cron_description ) )
                throw std::logic_error( std::string( "cron parse failed: " ) + cron_description );
            jobGroup->SetDescription( meta_description );
        }

        if ( ptree.count( "name" ) > 0 )
        {
            std::string name = ptree.get<std::string>( "name" );
            if ( check_name_existance && HasJobName( name ) )
                throw std::logic_error( std::string( "job name already exists: " ) + name );
            jobGroup->SetName( name );
        }

        // parse job files 
        bool succeeded = true;
        for( auto it = jobFiles.cbegin(); it != jobFiles.cend(); ++it )
        {
            // read job description from file
            std::string filePath = *it;
            if ( filePath[0] != '/' )
            {
                filePath = jobsDir_ + '/' + filePath;
            }

            std::ifstream file( filePath.c_str() );
            if ( !file.is_open() )
            {
                PLOG_ERR( "CreateMetaJob: couldn't open " << filePath );
                succeeded = false;
                break;
            }
            std::string jobDescr, line;
            while( getline( file, line ) )
                jobDescr += line;

            Job *job = CreateJob( jobDescr, check_name_existance );
            if ( job )
            {
                if ( jobs.empty() ) // first job must contain meta job description
                {
                    job->SetDescription( meta_description );
                }

                JobPtr jobPtr( job );
                jobFileToIndex[ *it ] = jobs.size();
                indexToJob.push_back( jobPtr );
                jobs.push_back( jobPtr );
            }
            else
            {
                PLOG_ERR( "JobManager::CreateMetaJob: CreateJob failed, job=" << *it );
                succeeded = false;
                break;
            }
        }

        if ( succeeded )
        {
            succeeded = PrepareJobGraph( ptree, jobFileToIndex, jobGroup );
        }

        if ( !succeeded )
        {
            jobs.clear();
        }

        return succeeded;
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "JobManager::CreateMetaJob exception: " << e.what() );
    }

    return false;
}

void JobManager::PushJob( JobPtr &job )
{
    RegisterJobName( job->GetName() );

    job->SetJobId( jobId_++ );
    jobs_->PushJob( job, numJobGroups_++ );

    PLOG( "JobManager::PushJob: jobId=" << job->GetJobId() );

    IJobEventReceiver *jobEventReceiver = common::GetService< IJobEventReceiver >();
    jobEventReceiver->OnJobAdd( std::to_string( job->GetJobId() ), job->GetDescription() );

    IScheduler *scheduler = common::GetService< IScheduler >();
    scheduler->OnNewJob();
    timeoutManager_->PushJobQueue( job->GetJobId(), job->GetQueueTimeout() );
}

void JobManager::PushJobs( std::list< JobPtr > &jobs )
{
    if ( jobs.empty() )
        return;

    const int groupId = numJobGroups_++;
    for( auto &job : jobs )
    {
        RegisterJobName( job->GetName() );
        job->SetJobId( jobId_++ );
    }
    jobs_->PushJobs( jobs, groupId );

    PLOG( "JobManager::PushJobs: groupId=" << groupId );

    IJobEventReceiver *jobEventReceiver = common::GetService< IJobEventReceiver >();
    JobPtr firstJob = *jobs.begin(); // first job has meta job description
    jobEventReceiver->OnJobAdd( std::to_string( firstJob->GetJobId() ), firstJob->GetDescription() );

    IScheduler *scheduler = common::GetService< IScheduler >();
    scheduler->OnNewJob();

    for( auto &job : jobs )
    {
        timeoutManager_->PushJobQueue( job->GetJobId(), job->GetQueueTimeout() );
    }
}

void JobManager::BuildAndPushJob( int64_t jobId, const std::string &jobDescription, bool plannedByCron )
{
    boost::property_tree::ptree ptree;
    JDLJson parser;
    if ( !parser.ParseJob( jobDescription, ptree ) )
        return;

    const bool cron = jobId < 0;
    const bool check_name_existance = !cron;

    if ( ptree.count( "graph" ) > 0 )
    {
        std::list< JobPtr > jobs;
        if ( CreateMetaJob( jobDescription, jobs, check_name_existance ) && !jobs.empty() )
        {
            if ( cron )
            {
                if ( !plannedByCron )
                {
                    ICronManager *cronManager = common::GetService< ICronManager >();
                    cronManager->PushMetaJob( jobs );
                    return;
                }
            }
            else
            {
                RegisterJobName( jobs.front()->GetJobGroup()->GetName() );
            }
            for( auto &job : jobs )
            {
                if ( cron )
                {
                    job->SetJobId( jobId_++ );
                }
                else
                {
                    RegisterJobName( job->GetName() );
                    if ( jobId >= jobId_ + 1 )
                    {
                        jobId_ = jobId + 1;
                    }
                    job->SetJobId( jobId++ );
                }
            }
            const int groupId = numJobGroups_++;
            jobs_->PushJobs( jobs, groupId );

            PLOG( "push jobs from history: groupId=" << groupId );

            IScheduler *scheduler = common::GetService< IScheduler >();
            scheduler->OnNewJob();

            for( const auto &job : jobs )
            {
                timeoutManager_->PushJobQueue( job->GetJobId(), job->GetQueueTimeout() );
            }
        }
    }
    else
    {
        JobPtr job( CreateJob( jobDescription, check_name_existance ) );
        if ( job )
        {
            if ( cron )
            {
                if ( !plannedByCron )
                {
                    ICronManager *cronManager = common::GetService< ICronManager >();
                    cronManager->PushJob( job, false );
                    return;
                }

                job->SetJobId( jobId_++ );
            }
            else
            {
                RegisterJobName( job->GetName() );
                if ( jobId >= jobId_ )
                {
                    jobId_ = jobId + 1;
                }
                job->SetJobId( jobId++ );
            }
            jobs_->PushJob( job, numJobGroups_++ );

            PLOG( "push job from history: jobId=" << job->GetJobId() );

            IScheduler *scheduler = common::GetService< IScheduler >();
            scheduler->OnNewJob();
            timeoutManager_->PushJobQueue( jobId, job->GetQueueTimeout() );
        }
    }
}

bool JobManager::GetJobById( int64_t jobId, JobPtr &job )
{
    return jobs_->GetJobById( jobId, job );
}

bool JobManager::DeleteJob( int64_t jobId )
{
    return jobs_->DeleteJob( jobId );
}

bool JobManager::DeleteJobGroup( int64_t groupId )
{
    return jobs_->DeleteJobGroup( groupId );
}

bool JobManager::DeleteNamedJob( const std::string &name )
{
    std::set< JobPtr > jobs;
    jobs_->GetJobsByName( name, jobs );
    for( const auto &job : jobs )
    {
        jobs_->DeleteJob( job->GetJobId() );
    }
    return !jobs.empty();
}

void JobManager::DeleteAllJobs()
{
    jobs_->Clear();
}

bool JobManager::PopJob( JobPtr &job )
{
    return jobs_->PopJob( job );
}

bool JobManager::RegisterJobName( const std::string &name )
{
    if ( name.empty() )
        return false;

    std::unique_lock< std::mutex > lock( jobNamesMut_ );
    if ( !jobNames_.insert( name ).second )
    {
        PLOG_ERR( "JobManager::RegisterJobName: couldn't register job name: " << name );
        return false;
    }
    return true;
}

bool JobManager::ReleaseJobName( const std::string &name )
{
    if ( name.empty() )
        return false;

    bool erased;
    {
        std::unique_lock< std::mutex > lock( jobNamesMut_ );
        erased = jobNames_.erase( name ) > 0;
    }

    if ( erased )
    {
        IJobEventReceiver *jobEventReceiver = common::GetService< IJobEventReceiver >();
        jobEventReceiver->OnJobDelete( name );
    }
    return erased;
}

bool JobManager::HasJobName( const std::string &name )
{
    if ( name.empty() )
        return false;

    std::unique_lock< std::mutex > lock( jobNamesMut_ );
    return jobNames_.find( name ) != jobNames_.end();
}

void JobManager::OnJobDependenciesResolved( const JobPtr &job )
{
    jobs_->OnJobDependenciesResolved( job );
}

JobManager &JobManager::SetTimeoutManager( ITimeoutManager *timeoutManager )
{
    timeoutManager_ = timeoutManager;
    return *this;
}

JobManager &JobManager::SetMasterId( const std::string &masterId )
{
    masterId_ = masterId;
    return *this;
}

JobManager &JobManager::SetExeDir( const std::string &exeDir )
{
    exeDir_ = exeDir;

    common::Config &cfg = common::Config::Instance();
    std::string jobsDir = cfg.Get<std::string>( "jobs_path" );
    if ( jobsDir.empty() || jobsDir[0] != '/' )
    {
        jobsDir = exeDir + '/' + jobsDir;
    }
    jobsDir_ = jobsDir;
    return *this;
}

void JobManager::Shutdown()
{
    jobs_->Clear();
}

bool JobManager::ReadScript( const std::string &filePath, std::string &script ) const
{
    std::ifstream file( filePath.c_str() );
    if ( !file.is_open() )
    {
        PLOG_ERR( "JobManager::ReadScript: couldn't open " << filePath );
        return false;
    }

    std::string data;
    file.seekg( 0, std::ios::end );
    data.resize( file.tellg() );
    file.seekg( 0, std::ios::beg );
    file.read( &data[0], data.size() );

    return common::EncodeBase64( data.c_str(), data.size(), script );
}

Job *JobManager::CreateJob( const boost::property_tree::ptree &ptree )
{
    Job *job = nullptr;

    try
    {
        std::string fileName = ptree.get<std::string>( "script" );
        if ( fileName.empty() )
        {
            PLOG_ERR( "JobManager::CreateJob: empty script file name" );
            return nullptr;
        }
        if ( fileName[0] != '/' )
        {
            fileName = jobsDir_ + '/' + fileName;
        }

        bool sendScript = ptree.get<bool>( "send_script" );

        std::string script;
        if ( sendScript && !ReadScript( fileName, script ) )
            return nullptr;

        std::string language = ptree.get<std::string>( "language" );
        int priority = ptree.get<int>( "priority" );
        int timeout = ptree.get<int>( "job_timeout" );
        int queueTimeout = ptree.get<int>( "queue_timeout" );
        int taskTimeout = ptree.get<int>( "task_timeout" );
        int maxFailedNodes = ptree.get<int>( "max_failed_nodes" );
        int numExec = ptree.get<int>( "num_execution" );
        int maxClusterInstances = ptree.get<int>( "max_cluster_instances" );
        int maxWorkerInstances = ptree.get<int>( "max_worker_instances" );
        bool exclusive = ptree.get<bool>( "exclusive" );
        bool noReschedule = ptree.get<bool>( "no_reschedule" );

        if ( taskTimeout < 0 )
            taskTimeout = -1;

        job = new Job( script, language,
                       priority, maxFailedNodes,
                       numExec, maxClusterInstances, maxWorkerInstances,
                       timeout, queueTimeout, taskTimeout,
                       exclusive, noReschedule );

        job->SetFilePath( fileName );

        if ( ptree.count( "max_exec_at_worker" ) > 0 )
        {
            int value = ptree.get<int>( "max_exec_at_worker" );
            job->SetMaxExecAtWorker( value );
        }

        if ( ptree.count( "exec_unit_type" ) > 0 )
        {
            std::string value = ptree.get<std::string>( "exec_unit_type" );
            if ( value == "cpu" )
                job->SetExecUnitType( ExecUnitType::CPU );
            else
            if ( value == "host" )
                job->SetExecUnitType( ExecUnitType::HOST );
            else
                throw std::logic_error( std::string( "unknown exec_unit_type: " ) + value );
        }

        if ( ptree.count( "hosts" ) > 0 )
        {
            auto lambda = [ job ]( const std::string &value ) -> void
                { job->AddHost( value ); };
            ReadList( ptree, "hosts", lambda );
        }

        if ( ptree.count( "hosts_blacklist" ) > 0 )
        {
            auto lambda = [ job ]( const std::string &value ) -> void
                { job->AddHostToBlacklist( value ); };
            ReadList( ptree, "hosts_blacklist", lambda );
        }

        if ( ptree.count( "groups" ) > 0 )
        {
            auto lambda = [ job ]( const std::string &value ) -> void
                { job->AddGroup( value ); };
            ReadList( ptree, "groups", lambda );
        }

        if ( ptree.count( "groups_blacklist" ) > 0 )
        {
            auto lambda = [ job ]( const std::string &value ) -> void
                { job->AddGroupToBlacklist( value ); };
            ReadList( ptree, "groups_blacklist", lambda );
        }

        if ( ptree.count( "cron" ) > 0 )
        {
            std::string cron_description = ptree.get<std::string>( "cron" );
            if ( !job->GetCron().Parse( cron_description ) )
                throw std::logic_error( std::string( "cron parse failed: " ) + cron_description );
        }

        if ( ptree.count( "name" ) > 0 )
        {
            std::string name = ptree.get<std::string>( "name" );
            job->SetName( name );
        }
        else
        {
            if ( job->GetCron() )
                throw std::logic_error( std::string( "cron job must have exclusive name" ) );
        }

        return job;
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "JobManager::CreateJob exception: " << e.what() );
    }

    delete job;
    return nullptr;
}

void JobManager::ReadList( const boost::property_tree::ptree &ptree, const char *property,
                           std::function< void (const std::string &) > callback ) const
{
    for( const auto &v : ptree.get_child( property ) )
    {
        callback( v.second.get_value< std::string >() );
    }
}

bool JobManager::PrepareJobGraph( const boost::property_tree::ptree &ptree,
                                  std::map< std::string, int > &jobFileToIndex,
                                  const JobGroupPtr &jobGroup ) const
{
    using namespace boost;

    // create graph
    JobGraph &graph = jobGroup->GetGraph();
    unsigned pairNum = 1;

    for( const auto &adjList : ptree.get_child( "graph" ) )
    {
        int v1, v2;
        int numIter = 0;

        for( const auto &item : adjList.second )
        {
            std::string job = item.second.get_value< std::string >();

            if ( numIter++ )
            {
                v2 = jobFileToIndex[ job ];
                add_edge( v1, v2, graph );
                v1 = v2;
            }
            else
            {
                v1 = jobFileToIndex[ job ];
            }
        }

        if ( numIter < 2 )
        {
            PLOG_ERR( "JobManager::PrepareJobGraph: invalid jobs adjacency list, pair " << pairNum );
            return false;
        }

        ++pairNum;
    }

    // validate graph
    {
        bool has_cycle = false;
        cycle_detector vis( has_cycle );
        depth_first_search( graph, visitor( vis ) );
        if ( has_cycle )
        {
            PLOG_ERR( "JobManager::PrepareJobGraph: job graph has cycle" );
            return false;
        }
    }

    typedef graph_traits<JobGraph>::vertex_iterator VertexIter;
    VertexIter i, i_end;
    for( tie( i, i_end ) = vertices( graph ); i != i_end; ++i )
    {
        JobPtr job = jobGroup->GetIndexToJob()[ *i ].lock();
        if ( job )
        {
            job->SetJobVertex( *i );
            int deps = in_degree( *i, graph );
            job->SetNumDepends( deps );
            job->SetJobGroup( jobGroup );
        }
    }

    return true;
}

} // namespace master

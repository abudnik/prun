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
#include <boost/foreach.hpp>
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

bool JDLJason::ParseJob( const std::string &job_description, boost::property_tree::ptree &ptree )
{
    std::istringstream ss( job_description );
    try
    {
        boost::property_tree::read_json( ss, ptree );
    }
    catch( boost::property_tree::json_parser::json_parser_error &e )
    {
        PLOG_ERR( "JDLJason::ParseJob read_json failed: " << e.what() );
        return false;
    }
    return true;
}

JobManager::JobManager()
: jobs_( new JobQueueImpl ),
 timeoutManager_( nullptr ),
 numJobGroups_( 0 ),
 jobId_( 0 )
{}

Job *JobManager::CreateJob( const std::string &job_description ) const
{
    boost::property_tree::ptree ptree;
    JDLJason parser;
    if ( !parser.ParseJob( job_description, ptree ) )
        return nullptr;

    Job *job = CreateJob( ptree );
    if ( job )
    {
        job->SetDescription( job_description );
    }
    return job;
}

bool JobManager::CreateMetaJob( const std::string &meta_description, std::list< JobPtr > &jobs )
{
    boost::property_tree::ptree ptree;
    JDLJason parser;
    if ( !parser.ParseJob( meta_description, ptree ) )
        return false;

    try
    {
        typedef std::set< std::string > StringSet;
        StringSet jobFiles;

        // read job description file pathes
        BOOST_FOREACH( const boost::property_tree::ptree::value_type &adjList,
                       ptree.get_child( "graph" ) )
        {
            BOOST_FOREACH( const boost::property_tree::ptree::value_type &item,
                           adjList.second )
            {
                std::string job = item.second.get_value< std::string >();
                jobFiles.insert( job );
            }
        }

        int index = 0;
        std::map< std::string, int > jobFileToIndex;

        IJobGroupEventReceiverPtr evReceiverPtr = static_cast< IJobGroupEventReceiver * >( this );
        auto jobGroup = std::make_shared< JobGroup >( evReceiverPtr );
        std::vector< JobWeakPtr > &indexToJob = jobGroup->GetIndexToJob();

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

            Job *job = CreateJob( jobDescr );
            if ( job )
            {
                if ( !index ) // first job must contain meta job description
                {
                    job->SetDescription( meta_description );
                }

                JobPtr jobPtr( job );
                jobFileToIndex[ *it ] = index++;
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
    PLOG( "push job" );
    job->SetJobId( jobId_++ );
    jobs_->PushJob( job, numJobGroups_++ );

    IJobEventReceiver *jobEventReceiver = common::GetService< IJobEventReceiver >();
    jobEventReceiver->OnJobAdd( job );

    IScheduler *scheduler = common::GetService< IScheduler >();
    scheduler->OnNewJob();
    timeoutManager_->PushJobQueue( job->GetJobId(), job->GetQueueTimeout() );
}

void JobManager::PushJobs( std::list< JobPtr > &jobs )
{
    if ( jobs.empty() )
        return;

    PLOG( "push jobs" );
    for( auto &job : jobs )
    {
        job->SetJobId( jobId_++ );
    }
    jobs_->PushJobs( jobs, numJobGroups_++ );

    IJobEventReceiver *jobEventReceiver = common::GetService< IJobEventReceiver >();
    jobEventReceiver->OnJobAdd( *jobs.begin() ); // first job must contain meta job description

    IScheduler *scheduler = common::GetService< IScheduler >();
    scheduler->OnNewJob();

    for( auto &job : jobs )
    {
        timeoutManager_->PushJobQueue( job->GetJobId(), job->GetQueueTimeout() );
    }
}

void JobManager::PushJobFromHistory( int64_t jobId, const std::string &jobDescription )
{
    boost::property_tree::ptree ptree;
    JDLJason parser;
    if ( !parser.ParseJob( jobDescription, ptree ) )
        return;

    if ( ptree.count( "graph" ) > 0 )
    {
        std::list< JobPtr > jobs;
        if ( CreateMetaJob( jobDescription, jobs ) && !jobs.empty() )
        {
            PLOG( "push jobs from history" );
            for( auto &job : jobs )
            {
                if ( jobId >= jobId_ + 1 )
                {
                    jobId_ = jobId + 1;
                }
                job->SetJobId( jobId++ );
            }
            jobs_->PushJobs( jobs, numJobGroups_++ );

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
        JobPtr job( CreateJob( jobDescription ) );
        if ( job )
        {
            PLOG( "push job from history" );
            if ( jobId >= jobId_ )
            {
                jobId_ = jobId + 1;
            }
            job->SetJobId( jobId );
            jobs_->PushJob( job, numJobGroups_++ );

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

void JobManager::DeleteAllJobs()
{
    jobs_->Clear();
}

bool JobManager::PopJob( JobPtr &job )
{
    return jobs_->PopJob( job );
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

Job *JobManager::CreateJob( const boost::property_tree::ptree &ptree ) const
{
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
        int maxClusterCPU = ptree.get<int>( "max_cluster_cpu" );
        int maxCPU = ptree.get<int>( "max_cpu" );
        bool exclusive = ptree.get<bool>( "exclusive" );
        bool noReschedule = ptree.get<bool>( "no_reschedule" );

        if ( taskTimeout < 0 )
            taskTimeout = -1;

        Job *job = new Job( script, language,
                            priority, maxFailedNodes,
                            numExec, maxClusterCPU, maxCPU,
                            timeout, queueTimeout, taskTimeout,
                            exclusive, noReschedule );

        job->SetFilePath( fileName );

        if ( ptree.count( "hosts" ) > 0 )
        {
            ReadHosts( job, ptree );
        }

        if ( ptree.count( "groups" ) > 0 )
        {
            ReadGroups( job, ptree );
        }

        return job;
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "JobManager::CreateJob exception: " << e.what() );
        return nullptr;
    }
}

void JobManager::ReadHosts( Job *job, const boost::property_tree::ptree &ptree ) const
{
    BOOST_FOREACH( const boost::property_tree::ptree::value_type &v,
                   ptree.get_child( "hosts" ) )
    {
        job->AddHost( v.second.get_value< std::string >() );
    }
}

void JobManager::ReadGroups( Job *job, const boost::property_tree::ptree &ptree ) const
{
    BOOST_FOREACH( const boost::property_tree::ptree::value_type &v,
                   ptree.get_child( "groups" ) )
    {
        job->AddGroup( v.second.get_value< std::string >() );
    }
}

bool JobManager::PrepareJobGraph( const boost::property_tree::ptree &ptree,
                                  std::map< std::string, int > &jobFileToIndex,
                                  std::shared_ptr< JobGroup > &jobGroup ) const
{
    using namespace boost;

    // create graph
    JobGraph &graph = jobGroup->GetGraph();
    unsigned pairNum = 1;

    BOOST_FOREACH( const boost::property_tree::ptree::value_type &adjList,
                   ptree.get_child( "graph" ) )
    {
        int v1, v2;
        int numIter = 0;

        BOOST_FOREACH( const boost::property_tree::ptree::value_type &item,
                       adjList.second )
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

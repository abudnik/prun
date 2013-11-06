#define BOOST_SPIRIT_THREADSAFE

#include <boost/property_tree/json_parser.hpp>
#include <boost/algorithm/string.hpp>
#include <iterator>
#include "job_manager.h"
#include "common/log.h"
#include "common/helper.h"
#include "scheduler.h"
#include "timeout_manager.h"

namespace master {

bool JDLJason::ParseJob( const std::string &job_description, boost::property_tree::ptree &ptree )
{
    std::stringstream ss;
    ss << job_description;
    try
    {
        boost::property_tree::read_json( ss, ptree );
    }
    catch( boost::property_tree::json_parser::json_parser_error &e )
    {
        PS_LOG( "JDLJason::ParseJob read_json failed: " << e.what() );
        return false;
    }
    return true;
}

Job *JobManager::CreateJob( const std::string &job_description ) const
{
    boost::property_tree::ptree ptree;
    JDLJason parser;
    if ( !parser.ParseJob( job_description, ptree ) )
        return NULL;

    return CreateJob( ptree );
}

void JobManager::CreateMetaJob( const std::string &meta_description, std::list< Job * > &jobs ) const
{
    std::istringstream ss( meta_description );
    std::string line;
    typedef std::set< std::string > StringSet;
    StringSet jobFiles;

    // read job description file pathes
    std::copy( std::istream_iterator<std::string>( ss ),
               std::istream_iterator<std::string>(),
               std::inserter< std::set< std::string > >( jobFiles, jobFiles.begin() ) );

    int index = 0;
    std::map< std::string, int > jobFileToIndex;
    std::map< int, Job * > indexToJob;

    // parse job files 
    StringSet::const_iterator it = jobFiles.begin();
    bool allParsed = true;
    for( ; it != jobFiles.end(); ++it )
    {
        // read job description from file
        std::string filePath = exeDir_ + '/' + *it;
        std::ifstream file( filePath.c_str() );
        if ( !file.is_open() )
        {
            PS_LOG( "CreateMetaJob: couldn't open " << filePath );
            allParsed = false;
            break;
        }
        std::string jobDescr, line;
        while( getline( file, line ) )
            jobDescr += line;

        master::Job *job = master::JobManager::Instance().CreateJob( jobDescr );
        if ( job )
        {
            jobFileToIndex[*it] = index;
            indexToJob[index] = job;
            ++index;
            jobs.push_back( job );
        }
        else
        {
            PS_LOG( "JobManager::CreateMetaJob: CreateJob failed, job=" << *it );
            allParsed = false;
            break;
        }
    }
    if ( allParsed )
    {
        TopologicalSort( ss, jobFileToIndex, indexToJob );
    }
    else
    {
        std::list< Job * >::iterator it = jobs.begin();
        for( ; it != jobs.end(); )
        {
            delete *it;
            jobs.erase( it++ );
        }
    }
}

void JobManager::PushJob( Job *job )
{
    PS_LOG( "push job" );
    jobs_.PushJob( job );

    Scheduler::Instance().OnNewJob( job );
    timeoutManager_->PushJobQueue( job->GetJobId(), job->GetQueueTimeout() );
}

Job *JobManager::GetJobById( int64_t jobId )
{
    return jobs_.GetJobById( jobId );
}

bool JobManager::DeleteJob( int64_t jobId )
{
    return jobs_.DeleteJob( jobId );
}

Job *JobManager::PopJob()
{
    return jobs_.PopJob();
}

Job *JobManager::GetTopJob()
{
    return jobs_.GetTopJob();
}

void JobManager::Initialize( const std::string &exeDir, TimeoutManager *timeoutManager )
{
    exeDir_ = exeDir;
    timeoutManager_ = timeoutManager;
}

void JobManager::Shutdown()
{
    jobs_.Clear();
}

bool JobManager::ReadScript( const std::string &fileName, std::string &script ) const
{
    std::string filePath = exeDir_ + '/' + fileName;
    std::ifstream file( filePath.c_str() );
    if ( !file.is_open() )
    {
        PS_LOG( "JobManager::ReadScript: couldn't open " << filePath );
        return false;
    }

    std::string data, line;
    while( std::getline( file, line ) )
    {
        boost::trim_right( line );
        data += line + '\n';
    }

    return common::EncodeBase64( data.c_str(), data.size(), script );
}

Job *JobManager::CreateJob( boost::property_tree::ptree &ptree ) const
{
    try
    {
        std::string fileName = ptree.get<std::string>( "script" );
        if ( fileName.empty() )
        {
            PS_LOG( "JobManager::CreateJob: empty script file name" );
            return NULL;
        }

        std::string script;
        if ( !ReadScript( fileName, script ) )
            return NULL;

        std::string language = ptree.get<std::string>( "language" );
        int timeout = ptree.get<int>( "job_timeout" );
        int queueTimeout = ptree.get<int>( "queue_timeout" );
        int taskTimeout = ptree.get<int>( "task_timeout" );
        int maxFailedNodes = ptree.get<int>( "max_failed_nodes" );
        int maxCPU = ptree.get<int>( "max_cpu" );
        bool noReschedule = ptree.get<bool>( "no_reschedule" );
        bool exclusiveExec = ptree.get<bool>( "exclusive_exec" );

        if ( taskTimeout < 0 )
            taskTimeout = -1;

        if ( maxCPU > 100 || maxCPU < 1 )
        {
            PS_LOG( "JobManager::CreateJob invalid maxCPU value=" << maxCPU <<
                    ", set maxCPU value to 100" );
            maxCPU = 100;
        }

        Job *job = new Job( script, language,
                            maxFailedNodes, maxCPU, timeout, queueTimeout, taskTimeout,
                            noReschedule, exclusiveExec );
        return job;
    }
    catch( std::exception &e )
    {
        PS_LOG( "JobManager::CreateJob exception: " << e.what() );
        return NULL;
    }
}

void JobManager::TopologicalSort( const std::istringstream &ss,
                                  const std::map< std::string, int > &jobFileToIndex,
                                  const std::map< int, Job * > &indexToJob) const
{
}

}

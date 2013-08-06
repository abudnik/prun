#include <boost/property_tree/json_parser.hpp>
#include <boost/algorithm/string.hpp>
#include "job_manager.h"
#include "common/log.h"
#include "sheduler.h"

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

bool JobManager::PushJob( const std::string &job_description )
{
    boost::property_tree::ptree ptree;
    JDLJason parser;
    if ( !parser.ParseJob( job_description, ptree ) )
        return false;

    Job *job = CreateJob( ptree );
    if ( !job )
        return false;

    PS_LOG( "push job" );
    jobs_.PushJob( job );

    Sheduler::Instance().OnNewJob( job );

    return true;
}

Job *JobManager::GetJobById( int64_t jobId )
{
    return jobs_.GetJobById( jobId );
}

Job *JobManager::PopJob()
{
    return jobs_.PopJob();
}

Job *JobManager::GetTopJob()
{
    return jobs_.GetTopJob();
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

    std::string line;
    while( std::getline( file, line ) )
    {
        boost::trim( line );
        script += line + '\n';
    }

    return true;
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
        int timeout = ptree.get<int>( "timeout" );
        std::string priority = ptree.get<std::string>( "priority" );
        int numNodes = ptree.get<int>( "num_nodes" );
        int maxFailedNodes = ptree.get<int>( "max_failed_nodes" );

        JobPriority jobPriority;
        if ( priority == "high" )
            jobPriority = JOB_PRIORITY_HIGH;
        else
            jobPriority = JOB_PRIORITY_LOW;

        Job *job = new Job( script.c_str(), language.c_str(), numNodes,
							maxFailedNodes, timeout, jobPriority );
        return job;
    }
    catch( std::exception &e )
    {
        PS_LOG( "JobManager::CreateJob exception: " << e.what() );
        return NULL;
    }
}

}

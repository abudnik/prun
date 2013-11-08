#ifndef __TEST_H
#define __TEST_H

#include <fstream>
#include "job_manager.h"


namespace master {

void TestSingleJob( const std::string &exeDir )
{
    // read job description from file
    std::string filePath = exeDir + "/test/test.job";
    std::ifstream file( filePath.c_str() );
    if ( !file.is_open() )
    {
        PS_LOG( "TestSingleJob: couldn't open " << filePath );
        return;
    }
    std::string jobDescr, line;
    while( getline( file, line ) )
        jobDescr += line;

    master::Job *job = master::JobManager::Instance().CreateJob( jobDescr );
    if ( job )
    {
        // add job to job queue
        master::JobManager::Instance().PushJob( job );
    }
}

void TestMetaJob( const std::string &exeDir )
{
    // read meta job description from file
    std::string filePath = exeDir + "/test/test.meta";
    std::ifstream file( filePath.c_str() );
    if ( !file.is_open() )
    {
        PS_LOG( "TestMetaJob: couldn't open " << filePath );
        return;
    }
    std::string metaDescr, line;
    while( getline( file, line ) )
        metaDescr += line + '\n';

    typedef std::list< master::Job * > JobList;
    JobList jobs;
    master::JobManager::Instance().CreateMetaJob( metaDescr, jobs );
    master::JobManager::Instance().PushJobs( jobs );

}

void RunTests( const std::string &exeDir )
{
    TestMetaJob( exeDir );
    TestMetaJob( exeDir );
    TestMetaJob( exeDir );
}

} // namespace master

#endif

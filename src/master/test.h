#ifndef __TEST_H
#define __TEST_H

#include <fstream>
#include "job_manager.h"


namespace master {

void TestSingleJob( const std::string &filePath )
{
    // read job description from file
    std::ifstream file( filePath.c_str() );
    if ( !file.is_open() )
    {
        PLOG( "TestSingleJob: couldn't open " << filePath );
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

void TestMetaJob( const std::string &filePath )
{
    // read meta job description from file
    std::ifstream file( filePath.c_str() );
    if ( !file.is_open() )
    {
        PLOG( "TestMetaJob: couldn't open " << filePath );
        return;
    }
    std::string metaDescr, line;
    while( getline( file, line ) )
        metaDescr += line + '\n';

    std::list< master::Job * > jobs;
    master::JobManager::Instance().CreateMetaJob( metaDescr, jobs );
    master::JobManager::Instance().PushJobs( jobs );

}

void RunTests( const std::string &exeDir )
{
    std::string filePath = exeDir + "/test/test.all";
    std::ifstream file( filePath.c_str() );
    if ( !file.is_open() )
    {
        PLOG( "RunTests: couldn't open " << filePath );
        return;
    }
    int i = 0;
    std::string line;
    while( getline( file, line ) )
    {
        size_t found = line.rfind( '.' );
        if ( found == std::string::npos )
        {
            PLOG( "RunTests: couldn't extract job file extension, line=" << i++ );
            continue;
        }
        std::string ext = line.substr( found + 1 );

        filePath = exeDir + "/test/" + line;

        if ( ext == "job" )
            TestSingleJob( filePath );
        else
        if ( ext == "meta" )
            TestMetaJob( filePath );
        else
            PLOG( "RunTests: unknown file extension, line=" << i );

        ++i;
    }
}

} // namespace master

#endif

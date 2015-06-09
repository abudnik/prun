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

#ifndef __JOB_MANAGER_H
#define __JOB_MANAGER_H

#include <boost/property_tree/ptree.hpp>
#include "job.h"


namespace master {

class JobDescriptionLanguage
{
public:
    virtual ~JobDescriptionLanguage() {}
    virtual bool ParseJob( const std::string &job_description, boost::property_tree::ptree &ptree ) = 0;
};

class JDLJason : public JobDescriptionLanguage
{
public:
    bool ParseJob( const std::string &job_description, boost::property_tree::ptree &ptree );
};

struct ITimeoutManager;

struct IJobManager
{
    virtual Job *CreateJob( const std::string &job_description, bool check_name_existance ) = 0;
    virtual bool CreateMetaJob( const std::string &meta_description, std::list< JobPtr > &jobs, bool check_name_existance ) = 0;
    virtual void PushJob( JobPtr &job ) = 0;
    virtual void PushJobs( std::list< JobPtr > &jobs ) = 0;
    virtual void BuildAndPushJob( int64_t jobId, const std::string &jobDescription ) = 0;

    virtual bool GetJobById( int64_t jobId, JobPtr &job ) = 0;
    virtual bool DeleteJob( int64_t jobId ) = 0;
    virtual bool DeleteJobGroup( int64_t groupId ) = 0;
    virtual bool DeleteNamedJob( const std::string &name ) = 0;
    virtual void DeleteAllJobs() = 0;

    virtual bool PopJob( JobPtr &job ) = 0;

    virtual bool RegisterJobName( const std::string &name ) = 0;
    virtual bool ReleaseJobName( const std::string &name ) = 0;

    virtual const std::string &GetMasterId() const = 0;
    virtual const std::string &GetJobsDir() const = 0;
};

class JobManager : public IJobManager, public IJobGroupEventReceiver
{
    typedef std::set< std::string > JobNames;

public:
    JobManager();

    // IJobManager
    virtual Job *CreateJob( const std::string &job_description, bool check_name_existance );
    virtual bool CreateMetaJob( const std::string &meta_description, std::list< JobPtr > &jobs, bool check_name_existance );
    virtual void PushJob( JobPtr &job );
    virtual void PushJobs( std::list< JobPtr > &jobs );
    virtual void BuildAndPushJob( int64_t jobId, const std::string &jobDescription );

    virtual bool GetJobById( int64_t jobId, JobPtr &job );
    virtual bool DeleteJob( int64_t jobId );
    virtual bool DeleteJobGroup( int64_t groupId );
    virtual bool DeleteNamedJob( const std::string &name );
    virtual void DeleteAllJobs();

    virtual bool PopJob( JobPtr &job );

    virtual bool RegisterJobName( const std::string &name );
    virtual bool ReleaseJobName( const std::string &name );

    virtual const std::string &GetMasterId() const { return masterId_; }
    virtual const std::string &GetJobsDir() const { return jobsDir_; }

    // IJobGroupEventReceiver
    virtual void OnJobDependenciesResolved( const JobPtr &job );

    JobManager &SetTimeoutManager( ITimeoutManager *timeoutManager );
    JobManager &SetMasterId( const std::string &masterId );
    JobManager &SetExeDir( const std::string &exeDir );

    void Shutdown();

private:
    bool ReadScript( const std::string &filePath, std::string &script ) const;
    Job *CreateJob( const boost::property_tree::ptree &ptree );

    void ReadList( const boost::property_tree::ptree &ptree, const char *property,
                   std::function< void (const std::string &) > callback ) const;

    bool HasJobName( const std::string &name );

    bool PrepareJobGraph( const boost::property_tree::ptree &ptree,
                          std::map< std::string, int > &jobFileToIndex,
                          const JobGroupPtr &jobGroup ) const;

private:
    std::shared_ptr< IJobQueue > jobs_;
    ITimeoutManager *timeoutManager_;
    std::string exeDir_, jobsDir_;
    std::string masterId_;
    int64_t numJobGroups_;
    int64_t jobId_;

    JobNames jobNames_;
    std::mutex jobNamesMut_;
};

} // namespace master

#endif

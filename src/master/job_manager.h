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
    virtual Job *CreateJob( const std::string &job_description ) const = 0;
    virtual void CreateMetaJob( const std::string &meta_description, std::list< JobPtr > &jobs ) const = 0;
    virtual void PushJob( Job *job ) = 0;
    virtual void PushJobs( std::list< JobPtr > &jobs ) = 0;

    virtual bool GetJobById( int64_t jobId, JobPtr &job ) = 0;
    virtual bool DeleteJob( int64_t jobId ) = 0;
    virtual bool DeleteJobGroup( int64_t groupId ) = 0;
    virtual void DeleteAllJobs() = 0;

    virtual bool PopJob( JobPtr &job ) = 0;

    virtual const std::string &GetMasterId() const = 0;
    virtual const std::string &GetJobsDir() const = 0;
};

class JobManager : public IJobManager
{
public:
    JobManager();

    virtual Job *CreateJob( const std::string &job_description ) const;
    virtual void CreateMetaJob( const std::string &meta_description, std::list< JobPtr > &jobs ) const;
    virtual void PushJob( Job *job );
    virtual void PushJobs( std::list< JobPtr > &jobs );

    virtual bool GetJobById( int64_t jobId, JobPtr &job );
    virtual bool DeleteJob( int64_t jobId );
    virtual bool DeleteJobGroup( int64_t groupId );
    virtual void DeleteAllJobs();

    virtual bool PopJob( JobPtr &job );

    virtual const std::string &GetMasterId() const { return masterId_; }
    virtual const std::string &GetJobsDir() const { return jobsDir_; }

    JobManager &SetTimeoutManager( ITimeoutManager *timeoutManager );
    JobManager &SetMasterId( const std::string &masterId );
    JobManager &SetExeDir( const std::string &exeDir );

    void Shutdown();

private:
    bool ReadScript( const std::string &filePath, std::string &script ) const;
    Job *CreateJob( const boost::property_tree::ptree &ptree ) const;
    void ReadHosts( Job *job, const boost::property_tree::ptree &ptree ) const;
    void ReadGroups( Job *job, const boost::property_tree::ptree &ptree ) const;

    bool PrepareJobGraph( std::istringstream &ss,
                          std::map< std::string, int > &jobFileToIndex,
                          boost::shared_ptr< JobGroup > &jobGroup ) const;

private:
    boost::shared_ptr< JobQueue > jobs_;
    ITimeoutManager *timeoutManager_;
    std::string exeDir_, jobsDir_;
    std::string masterId_;
    int64_t numJobGroups_;
};

} // namespace master

#endif

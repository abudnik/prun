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

#ifndef __EXEC_JOB_H
#define __EXEC_JOB_H

#include <boost/property_tree/ptree.hpp>

namespace worker {

class Job
{
public:
    Job()
    : errCode_( 0 )
    {}

    void OnError( int err ) { errCode_ = err; }
    int GetError() const { return errCode_; }

private:
    int errCode_;
};

class JobExec : public Job
{
public:
    void ParseRequest( boost::property_tree::ptree &ptree )
    {
        commId_ = ptree.get<int>( "id" );
        scriptLength_ = ptree.get<unsigned int>( "len" );
        language_ = ptree.get<std::string>( "lang" );
        filePath_ = ptree.get<std::string>( "path" );
        jobId_ = ptree.get<int64_t>( "job_id" );
        taskId_ = ptree.get<int>( "task_id" );
        masterId_ = ptree.get<std::string>( "master_id" );

        numTasks_ = ptree.get<int>( "num_tasks" );
        timeout_ = ptree.get<int>( "timeout" );

        fromFile_ = !scriptLength_;
    }

    void SetScriptLength( unsigned int len ) { scriptLength_ = len; }
    void SetExecTime( int64_t time ) { execTime_ = time; }

    int GetCommId() const { return commId_; }
    unsigned int GetScriptLength() const { return scriptLength_; }
    const std::string &GetScriptLanguage() const { return language_; }
    const std::string &GetFilePath() const { return filePath_; }
    int64_t GetJobId() const { return jobId_; }
    int GetTaskId() const { return taskId_; }
    const std::string &GetMasterId() const { return masterId_; }
    int GetNumTasks() const { return numTasks_; }
    int GetTimeout() const { return timeout_; }
    bool IsFromFile() const { return fromFile_; }
    int64_t GetExecTime() const { return execTime_; }

private:
    int commId_;
    unsigned int scriptLength_;
    std::string language_;
    std::string filePath_;
    int64_t jobId_;
    int taskId_;
    std::string masterId_;
    int numTasks_;
    int timeout_;
    bool fromFile_;
    int64_t execTime_;
};

class JobStopTask : public Job
{
public:
    JobStopTask() {}

    JobStopTask( int64_t jobId, int taskId, const std::string &masterId )
    : jobId_( jobId ), taskId_( taskId ), masterId_( masterId )
    {}

    void ParseRequest( boost::property_tree::ptree &ptree )
    {
        jobId_ = ptree.get<int64_t>( "job_id" );
        taskId_ = ptree.get<int>( "task_id" );
        masterId_ = ptree.get<std::string>( "master_id" );
    }

    int64_t GetJobId() const { return jobId_; }
    int GetTaskId() const { return taskId_; }
    const std::string &GetMasterId() const { return masterId_; }

private:
    int64_t jobId_;
    int taskId_;
    std::string masterId_;
};

class JobStopPreviousJobs : public Job
{
public:
    void ParseRequest( boost::property_tree::ptree &ptree )
    {
        masterId_ = ptree.get<std::string>( "master_id" );
    }

    const std::string &GetMasterId() const { return masterId_; }

private:
    std::string masterId_;
};

} // namespace worker

#endif

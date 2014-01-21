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

#ifndef __WORKER_JOB_H
#define __WORKER_JOB_H

#define BOOST_SPIRIT_THREADSAFE

#include <boost/property_tree/json_parser.hpp>
#include "common/protocol.h"
#include "common/helper.h"
#include "common/request.h"
#include "common/error_code.h"
#include "job_completion_table.h"

namespace worker {

class Job
{
public:
    virtual void GetResponse( std::string &response ) const = 0;
    virtual std::string GetTaskType() const = 0;

    void OnError( int err )
    {
        errCode_ = err;
    }

    void SetMasterIP( const std::string &ip ) { masterIP_ = ip; }

    int GetErrorCode() const { return errCode_; }

    const std::string &GetMasterIP() const { return masterIP_; }

protected:
    virtual bool ParseRequestBody( const std::string &body, common::Protocol *parser ) = 0;

protected:
    int errCode_;
    std::string masterIP_;

    friend class RequestParser;
};

typedef boost::shared_ptr< Job > JobPtr;

class JobExec : public Job
{
public:
    typedef std::set<int> Tasks;

public:
    virtual std::string GetTaskType() const { return "exec"; }

    void SetFilePath( const std::string &path ) { filePath_ = path; }

    const Tasks &GetTasks() const { return tasks_; }
    const std::string &GetMasterId() const { return masterId_; }

    unsigned int GetScriptLength() const { return scriptLength_; }
    const std::string &GetScriptLanguage() const { return language_; }
    const std::string &GetScript() const { return script_; }
    const std::string &GetFilePath() const { return filePath_; }

    int GetNumTasks() const { return numTasks_; }
    int GetTimeout() const { return timeout_; }

protected:
    virtual bool ParseRequestBody( const std::string &body, common::Protocol *parser )
    {
        std::string script64;
        parser->ParseSendScript( body, language_, script64, filePath_,
                                 masterId_, jobId_, tasks_, numTasks_, timeout_ );
        if ( !common::DecodeBase64( script64, script_ ) )
            return false;

        scriptLength_ = script_.size();
        return true;
    }

    virtual void GetResponse( std::string &response ) const {}

protected:
    std::string masterId_;
    int64_t jobId_;
    Tasks tasks_;

    unsigned int scriptLength_;
    std::string language_;
    std::string script_;
    std::string filePath_;

    int numTasks_;
    int timeout_;
};

class JobGetResult : public Job
{
public:
    virtual std::string GetTaskType() const { return "get_result"; }

    const std::string &GetMasterId() const { return masterId_; }
    int64_t GetJobId() const { return jobId_; }
    int GetTaskId() const { return taskId_; }

protected:
    virtual bool ParseRequestBody( const std::string &body, common::Protocol *parser )
    {
        return parser->ParseGetJobResult( body, masterId_, jobId_, taskId_ );
    }

    virtual void GetResponse( std::string &response ) const
    {
        JobDescriptor descr;
        JobCompletionStat stat;
        common::ProtocolJson protocol;

        descr.jobId = GetJobId();
        descr.taskId = GetTaskId();
        descr.masterIP = GetMasterIP();
        descr.masterId = GetMasterId();
        if ( JobCompletionTable::Instance().Get( descr, stat ) )
        {
            JobCompletionTable::Instance().Erase( descr );
            protocol.SendJobResult( response, stat.errCode, stat.execTime );
        }
        else
        {
            JobCompletionTable::Instance().ErasePending( descr );

            PLOG( "Job::GetResponse: job not found in completion table: "
                  "jobId=" << GetJobId() << ", taskId=" << GetTaskId() <<
                  ", masterIP=" << GetMasterIP() << ", masterId=" << GetMasterId() );
            protocol.SendJobResult( response, NODE_JOB_COMPLETION_NOT_FOUND, 0 );
        }
    }

protected:
    std::string masterId_;
    int64_t jobId_;
    int taskId_;
};

class JobStopTask : public Job
{
public:
    virtual std::string GetTaskType() const { return "stop_task"; }

    const std::string &GetMasterId() const { return masterId_; }
    int64_t GetJobId() const { return jobId_; }
    int GetTaskId() const { return taskId_; }

protected:
    virtual bool ParseRequestBody( const std::string &body, common::Protocol *parser )
    {
        return parser->ParseStopTask( body, masterId_, jobId_, taskId_ );
    }

    virtual void GetResponse( std::string &response ) const
    {
        common::ProtocolJson protocol;
        protocol.SendCommandResult( response, GetErrorCode() );
    }

protected:
    std::string masterId_;
    int64_t jobId_;
    int taskId_;
};

class JobStopPreviousTask : public Job
{
public:
    virtual std::string GetTaskType() const { return "stop_prev"; }

    const std::string &GetMasterId() const { return masterId_; }

protected:
    virtual bool ParseRequestBody( const std::string &body, common::Protocol *parser )
    {
        return parser->ParseStopPreviousJobs( body, masterId_ );
    }

    virtual void GetResponse( std::string &response ) const
    {
        common::ProtocolJson protocol;
        protocol.SendCommandResult( response, GetErrorCode() );
    }

protected:
    std::string masterId_;
};

class JobStopAll : public Job
{
public:
    virtual std::string GetTaskType() const { return "stop_all"; }

protected:
    virtual bool ParseRequestBody( const std::string &body, common::Protocol *parser )
    {
        return true;
    }

    virtual void GetResponse( std::string &response ) const
    {
        common::ProtocolJson protocol;
        protocol.SendCommandResult( response, GetErrorCode() );
    }
};


} // namespace worker

#endif

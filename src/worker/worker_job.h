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
    typedef std::set<int> Tasks;

public:
    void GetResponse( std::string &response ) const
    {
        if ( taskType_ == "get_result" )
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
        else
        if ( taskType_ == "stop_task" || taskType_ == "stop_prev" || taskType_ == "stop_all" )
        {
            common::ProtocolJson protocol;
            protocol.SendCommandResult( response, GetErrorCode() );
        }
    }

    void OnError( int err )
    {
        errCode_ = err;
    }

    void SetMasterIP( const std::string &ip ) { masterIP_ = ip; }
    void SetMasterId( const std::string &id ) { masterId_ = id; }
    void SetFilePath( const std::string &path ) { filePath_ = path; }

    unsigned int GetScriptLength() const { return scriptLength_; }
    const std::string &GetScriptLanguage() const { return language_; }
    const std::string &GetScript() const { return script_; }
    const std::string &GetFilePath() const { return filePath_; }
    int64_t GetJobId() const { return jobId_; }
    int GetTaskId() const { return taskId_; }
    const Tasks &GetTasks() const { return tasks_; }
    int GetNumTasks() const { return numTasks_; }
    int GetTimeout() const { return timeout_; }
    int GetErrorCode() const { return errCode_; }

    virtual std::string GetTaskType() const = 0;

    const std::string &GetMasterIP() const { return masterIP_; }
    const std::string &GetMasterId() const { return masterId_; }

protected:
    virtual bool ParseRequestBody( const std::string &body, common::Protocol *parser )
    {
        if ( taskType_ == "get_result" )
        {
            return parser->ParseGetJobResult( body, masterId_, jobId_, taskId_ );
        }
        if ( taskType_ == "stop_task" )
        {
            return parser->ParseStopTask( body, masterId_, jobId_, taskId_ );
        }
        if ( taskType_ == "stop_prev" )
        {
            return parser->ParseStopPreviousJobs( body, masterId_ );
        }
        if ( taskType_ == "stop_all" )
        {
            return true;
        }
        return false;
    }

protected:
    unsigned int scriptLength_;
    std::string language_;
    std::string script_;
    std::string filePath_;
    int64_t jobId_;
    Tasks tasks_;
    int taskId_;
    int numTasks_;
    int timeout_;
    int errCode_;
    std::string taskType_;
    std::string masterIP_, masterId_;

    friend class RequestParser;
};

class JobExec : public Job
{
public:
    virtual std::string GetTaskType() const { return "exec"; }

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
};

} // namespace worker

#endif

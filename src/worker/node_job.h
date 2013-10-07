#ifndef __NODE_JOB_H
#define __NODE_JOB_H

#define BOOST_SPIRIT_THREADSAFE

#include <boost/property_tree/json_parser.hpp>
#include "common/protocol.h"
#include "common/helper.h"
#include "common/request.h"
#include "common/error_code.h"
#include "job_completion_table.h"

namespace python_server {

class Job
{
public:
    std::vector<int> Tasks;

public:
    template< typename T >
    bool ParseRequest( Request<T> &request )
    {
        const std::string &req = request.GetString();

        std::string protocol, header, body;
        int version;
        if ( !Protocol::ParseMsg( req, protocol, version, header, body ) )
        {
            PS_LOG( "Job::ParseRequest: couldn't parse request: " << req );
            return false;
        }

        ProtocolCreator protocolCreator;
        boost::scoped_ptr< Protocol > parser(
            protocolCreator.Create( protocol, version )
        );
        if ( !parser )
        {
            PS_LOG( "Job::ParseRequest: appropriate parser not found for protocol: "
                    << protocol << " " << version );
            return false;
        }

        parser->ParseMsgType( header, taskType_ );

        return ParseRequestBody( body, parser.get() );
    }

    void GetResponse( std::string &response ) const
    {
        if ( taskType_ == "get_result" )
        {
            JobDescriptor descr;
            JobCompletionStat stat;
            ProtocolJson protocol;

            descr.jobId = GetJobId();
            descr.taskId = GetTaskId();
            descr.masterIP = GetMasterIP();
            if ( JobCompletionTable::Instance().Get( descr, stat ) )
            {
                JobCompletionTable::Instance().Erase( descr );
                protocol.SendJobResult( response, stat.errCode );
            }
            else
            {
                PS_LOG( "Job::GetResponse: job not found in completion table: "
                        "jobId=" << GetJobId() << ", taskId=" << GetTaskId() <<
                        ", masterIP=" << GetMasterIP() );
                protocol.SendJobResult( response, NODE_JOB_COMPLETION_NOT_FOUND );
            }
        }
    }

    void OnError( int err )
    {
        errCode_ = err;
    }



    void SetMasterIP( const std::string &ip ) { masterIP_ = ip; }

    unsigned int GetScriptLength() const { return scriptLength_; }
    const std::string &GetScriptLanguage() const { return language_; }
    const std::string &GetScript() const { return script_; }
    int64_t GetJobId() const { return jobId_; }
    const Tasks &GetTasks() const { return tasks_; }
    int GetNumTasks() const { return numTasks_; }
    int GetTimeout() const { return timeout_; }
    int GetErrorCode() const { return errCode_; }
    const std::string &GetTaskType() const { return taskType_; }
    const std::string &GetMasterIP() const { return masterIP_; }

private:
    bool ParseRequestBody( const std::string &body, Protocol *parser )
    {
        if ( taskType_ == "exec" )
        {
            std::string script64;
            parser->ParseSendScript( body, language_, script64, jobId_, tasks_, numTasks_, timeout_ );
            if ( !DecodeBase64( script64, script_ ) )
                return false;

            scriptLength_ = script_.size();
            return true;
        }
        if ( taskType_ == "get_result" )
        {
            return parser->ParseGetJobResult( body, jobId_, taskId_ );
        }
        return false;
    }

private:
    unsigned int scriptLength_;
    std::string language_;
    std::string script_;
    int64_t jobId_;
    Tasks tasks_;
    int numTasks_;
    int timeout_;
    int errCode_;
    std::string taskType_;
    std::string masterIP_;
};

} // namespace python_server

#endif

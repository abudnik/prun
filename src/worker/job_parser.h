#ifndef __WORKER_PARSER_H
#define __WORKER_PARSER_H

#include "worker_job.h"

namespace worker {

class JobCreator
{
public:
    virtual Job *Create( const std::string &taskType )
    {
        if ( taskType == "exec" )
            return new JobExec();
        if ( taskType == "get_result" )
            return new JobGetResult();
        if ( taskType == "stop_task" )
            return new JobStopTask();
        if ( taskType == "stop_prev" )
            return new JobStopPreviousTask();
        if ( taskType == "stop_all" )
            return new JobStopAll();
        return NULL;
    }
};

class RequestParser
{
public:
    template< typename T >
    bool ParseRequest( /*const */common::Request<T> &request, JobPtr &job )
    {
        const std::string &req = request.GetString();

        std::string protocol, header, body;
        int version;
        if ( !common::Protocol::ParseMsg( req, protocol, version, header, body ) )
        {
            PLOG_ERR( "RequestParser::ParseRequest: couldn't parse request: " << req );
            return false;
        }

        common::ProtocolCreator protocolCreator;
        boost::scoped_ptr< common::Protocol > parser(
            protocolCreator.Create( protocol, version )
        );
        if ( !parser )
        {
            PLOG_ERR( "RequestParser::ParseRequest: appropriate parser not found for protocol: "
                      << protocol << " " << version );
            return false;
        }

        std::string taskType;
        parser->ParseMsgType( header, taskType );

        JobCreator jobCreator;
        Job *j = jobCreator.Create( taskType );
        if ( j )
        {
            job.reset( j );
            if ( job->ParseRequestBody( body, parser.get() ) )
            {
                return true;
            }
            else
            {
                PLOG_ERR( "1" );
            }
        }
        else
        {
            PLOG_ERR( "2" );
        }

        return false;
    }
};

} // namespace worker

#endif

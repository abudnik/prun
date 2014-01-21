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
        return NULL;
    }
};

class RequestParser
{
public:
    template< typename T >
    Job *ParseRequest( /*const */common::Request<T> &request )
    {
        const std::string &req = request.GetString();

        std::string protocol, header, body;
        int version;
        if ( !common::Protocol::ParseMsg( req, protocol, version, header, body ) )
        {
            PLOG_ERR( "Job::ParseRequest: couldn't parse request: " << req );
            return false;
        }

        common::ProtocolCreator protocolCreator;
        boost::scoped_ptr< common::Protocol > parser(
            protocolCreator.Create( protocol, version )
        );
        if ( !parser )
        {
            PLOG_ERR( "Job::ParseRequest: appropriate parser not found for protocol: "
                      << protocol << " " << version );
            return false;
        }

        std::string taskType;
        parser->ParseMsgType( header, taskType );

        JobCreator jobCreator;
        Job *job = jobCreator.Create( taskType );
        if ( job )
        {
            if ( job->ParseRequestBody( body, parser.get() ) )
            {
                return job;
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

        return NULL;
    }
};

} // namespace worker

#endif

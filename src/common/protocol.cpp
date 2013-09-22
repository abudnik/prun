#define BOOST_SPIRIT_THREADSAFE

#include <sstream>
#include <stdint.h> // boost/atomic/atomic.hpp:202:16: error: ‘uintptr_t’ was not declared in this scope
#include <boost/property_tree/json_parser.hpp>
#include <boost/lexical_cast.hpp>
#include "protocol.h"
#include "log.h"

namespace python_server {


void Protocol::AddHeader( std::string &msg )
{
    std::string header( GetProtocolType() );
    header += '\n';
    header += GetProtocolVersion();
    header += '\n';

    size_t size = header.size() + msg.size();
    std::ostringstream ss;
    ss << size << '\n' << header;
    msg.insert( 0, ss.str() );
}

bool Protocol::ParseMsg( const std::string &msg, std::string &protocol, int &version,
                         std::string &header, std::string &body )
{
    int size;
    try
    {
        std::istringstream ss( msg );
        ss >> size >> protocol >> version >> header >> body;
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseMsg: " << e.what() );
        return false;
    }
    return true;
}


bool ProtocolJson::NodePing( std::string &msg, const std::string &host )
{
    msg = std::string( "{\"type\":\"ping\"}\n" );
    msg += std::string( "{\"host\":\"" ) + host + "\"}";
    AddHeader( msg );
    return true;
}

bool ProtocolJson::NodeResponsePing( std::string &msg, int numCPU )
{
    msg = std::string( "{\"type\":\"ping_response\"}\n" );
    msg += std::string( "{\"num_cpu\":" ) + boost::lexical_cast<std::string>( numCPU ) + "}";
    AddHeader( msg );
    return true;
}

bool ProtocolJson::ParseResponsePing( const std::string &msg, int &numCPU )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        numCPU = ptree.get<int>( "num_cpu" );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseResponsePing: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::NodeJobCompletionPing( std::string &msg, int64_t jobId, int taskId )
{
    msg = std::string( "{\"type\":\"job_completion\"}\n" );
    msg += std::string( "{\"job_id\":" ) + boost::lexical_cast<std::string>( jobId ) + ","
        "\"task_id\":" + boost::lexical_cast<std::string>( taskId ) + "}";
    AddHeader( msg );
    return true;
}

bool ProtocolJson::ParseJobCompletionPing( const std::string &msg, int64_t &jobId, int &taskId )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        jobId = ptree.get<int64_t>( "job_id" );
        taskId = ptree.get<int>( "task_id" );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseJobCompletionPing: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::SendScript( std::string &msg, const std::string &scriptLanguage,
                               const std::string &script, int64_t jobId, int taskId, int numTasks,
                               int numCPU, int timeout )
{
    msg = std::string( "{\"type\":\"exec\"}\n" );
    msg += std::string( "{\"lang\":\"" ) + scriptLanguage + "\",\"script\":\"" + script + "\","
        "\"job_id\":" + boost::lexical_cast<std::string>( jobId ) + ","
        "\"task_id\":" + boost::lexical_cast<std::string>( taskId ) + ","
        "\"num_tasks\":" + boost::lexical_cast<std::string>( numTasks ) + ","
        "\"num_cpu\":" + boost::lexical_cast<std::string>( numCPU ) + ","
        "\"timeout\":" + boost::lexical_cast<std::string>( timeout ) + "}";
    AddHeader( msg );
    return true;
}

bool ProtocolJson::ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                    std::string &script, int64_t &jobId, int &taskId, int &numTasks,
                                    int &numCPU, int &timeout )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        scriptLanguage = ptree.get<std::string>( "lang" );
        script = ptree.get<std::string>( "script" );
        jobId = ptree.get<int64_t>( "job_id" );
        taskId = ptree.get<int>( "task_id" );
        numTasks = ptree.get<int>( "num_tasks" );
        numCPU = ptree.get<int>( "num_cpu" );
        timeout = ptree.get<int>( "timeout" );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseSendScript: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::GetJobResult( std::string &msg, int64_t jobId, int taskId )
{
    msg = std::string( "{\"type\":\"get_result\"}\n" );
    msg += std::string( "{\"job_id\":" ) + boost::lexical_cast<std::string>( jobId ) + ","
        "\"task_id\":" + boost::lexical_cast<std::string>( taskId ) + "}";
    AddHeader( msg );
    return true;
}

bool ProtocolJson::ParseGetJobResult( const std::string &msg, int64_t &jobId, int &taskId )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        jobId = ptree.get<int64_t>( "job_id" );
        taskId = ptree.get<int>( "task_id" );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseGetJobResult: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::SendJobResult( std::string &msg, int errCode )
{
    msg = std::string( "{\"type\":\"send_job_result\"}\n" );
    msg += std::string( "{\"err_code\":" ) + boost::lexical_cast<std::string>( errCode ) + "}";
    AddHeader( msg );
    return true;
}

bool ProtocolJson::ParseJobResult( const std::string &msg, int &errCode )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        errCode = ptree.get<int>( "err_code" );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseMsgType: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::ParseMsgType( const std::string &msg, std::string &type )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        type = ptree.get<std::string>( "type" );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseMsgType: " << e.what() );
        return false;
    }

    return true;
}

} // namespace python_server

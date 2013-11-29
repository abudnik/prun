#define BOOST_SPIRIT_THREADSAFE

#include <algorithm>
#include <cctype> // isspace
#include <sstream>
#include <stdint.h> // boost/atomic/atomic.hpp:202:16: error: ‘uintptr_t’ was not declared in this scope
#include <boost/property_tree/json_parser.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>
#include "protocol.h"
#include "log.h"

namespace common {


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
    std::ostringstream ss;
    boost::property_tree::ptree ptree;
    try
    {
        ptree.put( "host", host );
        boost::property_tree::write_json( ss, ptree, false );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::NodePing: " << e.what() );
        return false;
    }
    msg = std::string( "{\"type\":\"ping\"}\n" );
    msg += ss.str();
    AddHeader( msg );
    return true;
}

bool ProtocolJson::NodeResponsePing( std::string &msg, int numCPU, int64_t memSizeMb )
{
    std::ostringstream ss;
    boost::property_tree::ptree ptree;
    try
    {
        ptree.put( "num_cpu", numCPU );
        ptree.put( "mem_size", memSizeMb );
        boost::property_tree::write_json( ss, ptree, false );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::NodeResponsePing: " << e.what() );
        return false;
    }
    msg = std::string( "{\"type\":\"ping_response\"}\n" );
    msg += ss.str();
    AddHeader( msg );
    return true;
}

bool ProtocolJson::ParseResponsePing( const std::string &msg, int &numCPU, int64_t &memSizeMb )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        numCPU = ptree.get<int>( "num_cpu" );
        memSizeMb = ptree.get<int64_t>( "mem_size" );
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
    std::ostringstream ss;
    boost::property_tree::ptree ptree;
    try
    {
        ptree.put( "job_id", jobId );
        ptree.put( "task_id", taskId );
        boost::property_tree::write_json( ss, ptree, false );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::NodeJobCompletionPing: " << e.what() );
        return false;
    }
    msg = std::string( "{\"type\":\"job_completion\"}\n" );
    msg += ss.str();
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
                               const std::string &script, const std::string &filePath,
                               const std::string &masterId,
                               int64_t jobId, const std::set<int> &tasks,
                               int numTasks, int timeout )
{
    std::ostringstream ss;
    boost::property_tree::ptree ptree, child, element;
    try
    {
        ptree.put( "lang", scriptLanguage );
        ptree.put( "script", script );
        ptree.put( "file_path", filePath );
        ptree.put( "master_id", masterId );
        ptree.put( "job_id", jobId );
        ptree.put( "num_tasks", numTasks );
        ptree.put( "timeout", timeout );

        std::set<int>::const_iterator it = tasks.begin();
        for( ; it != tasks.end(); ++it )
        {
            element.put_value( *it );
            child.push_back( std::make_pair( "", element ) );
        }
        ptree.add_child( "tasks", child );

        boost::property_tree::write_json( ss, ptree, false );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::SendScript: " << e.what() );
        return false;
    }
    msg = ss.str();
    // cratch for boost bug with unexepected whitespaces in arrays:  "val": [   <whitespaces> ]
    msg.erase( std::remove_if( msg.begin(), msg.end(), isspace ), msg.end() );
    msg = std::string( "{\"type\":\"exec\"}\n" ) + msg;

    AddHeader( msg );
    return true;
}

bool ProtocolJson::ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                    std::string &script, std::string &filePath,
                                    std::string &masterId,
                                    int64_t &jobId, std::set<int> &tasks,
                                    int &numTasks, int &timeout )
{
    std::istringstream ss( msg );
    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        scriptLanguage = ptree.get<std::string>( "lang" );
        script = ptree.get<std::string>( "script" );
        filePath = ptree.get<std::string>( "file_path" );
        masterId = ptree.get<std::string>( "master_id" );
        jobId = ptree.get<int64_t>( "job_id" );
        numTasks = ptree.get<int>( "num_tasks" );
        timeout = ptree.get<int>( "timeout" );

        BOOST_FOREACH( const boost::property_tree::ptree::value_type &v,
                       ptree.get_child( "tasks" ) )
        {
            tasks.insert( v.second.get_value< int >() );
        }
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseSendScript: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::GetJobResult( std::string &msg, const std::string &masterId, int64_t jobId, int taskId )
{
    std::ostringstream ss;
    boost::property_tree::ptree ptree;
    try
    {
        ptree.put( "master_id", masterId );
        ptree.put( "job_id", jobId );
        ptree.put( "task_id", taskId );
        boost::property_tree::write_json( ss, ptree, false );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::GetJobResult: " << e.what() );
        return false;
    }
    msg = std::string( "{\"type\":\"get_result\"}\n" );
    msg += ss.str();
    AddHeader( msg );
    return true;
}

bool ProtocolJson::ParseGetJobResult( const std::string &msg, std::string &masterId, int64_t &jobId, int &taskId )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        masterId = ptree.get<std::string>( "master_id" );
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

bool ProtocolJson::SendJobResult( std::string &msg, int errCode, int64_t execTime )
{
    std::ostringstream ss;
    boost::property_tree::ptree ptree;
    try
    {
        ptree.put( "err_code", errCode );
        ptree.put( "elapsed", execTime );
        boost::property_tree::write_json( ss, ptree, false );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::SendJobResult: " << e.what() );
        return false;
    }
    msg = std::string( "{\"type\":\"send_job_result\"}\n" );
    msg += ss.str();
    AddHeader( msg );
    return true;
}

bool ProtocolJson::ParseJobResult( const std::string &msg, int &errCode, int64_t &execTime )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        errCode = ptree.get<int>( "err_code" );
        execTime = ptree.get<int64_t>( "elapsed" );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseMsgType: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::SendCommand( std::string &msg, const std::string &masterId, const std::string &command,
                                const std::list< std::pair< std::string, std::string > > &params )
{
    std::ostringstream ss;
    boost::property_tree::ptree ptree;
    try
    {
        ptree.put( "master_id", masterId );

        std::list< std::pair< std::string, std::string > >::const_iterator it = params.begin();
        for( ; it != params.end(); ++it )
        {
            ptree.put( it->first, it->second );
        }
        boost::property_tree::write_json( ss, ptree, false );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::SendCommand: " << e.what() );
        return false;
    }
    msg = std::string( "{\"type\":\"" ) + command + std::string( "\"}\n" );
    msg += ss.str();
    AddHeader( msg );
    return true;
}

bool ProtocolJson::SendCommandResult( std::string &msg, int errCode )
{
    std::ostringstream ss;
    boost::property_tree::ptree ptree;
    try
    {
        ptree.put( "err_code", errCode );
        boost::property_tree::write_json( ss, ptree, false );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::SendCommandResult: " << e.what() );
        return false;
    }
    msg = std::string( "{\"type\":\"send_command_result\"}\n" );
    msg += ss.str();
    AddHeader( msg );
    return true;
}

bool ProtocolJson::ParseSendCommandResult( const std::string &msg, int &errCode )
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
        PS_LOG( "ProtocolJson::ParseSendCommandResult: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::ParseStopTask( const std::string &msg, std::string &masterId, int64_t &jobId, int &taskId )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        masterId = ptree.get<std::string>( "master_id" );
        jobId = ptree.get<int64_t>( "job_id" );
        taskId = ptree.get<int>( "task_id" );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseStopTask: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::ParseStopPreviousJobs( const std::string &msg, std::string &masterId )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        masterId = ptree.get<std::string>( "master_id" );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseStopPreviousJobs: " << e.what() );
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

} // namespace common

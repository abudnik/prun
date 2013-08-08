#include <sstream>
#include <boost/property_tree/json_parser.hpp>
#include <boost/lexical_cast.hpp>
#include "protocol.h"
#include "log.h"

namespace python_server {


bool ProtocolJson::NodePing( std::string &msg, const std::string &host )
{
	msg = std::string( "{\"type\":\"ping\",\"host\":\"" ) + host + "\"}";
	AddHeader( msg );
	return true;
}

bool ProtocolJson::NodeResponsePing( std::string &msg )
{
	msg = std::string( "{\"type\":\"ping_response\"}" );
	AddHeader( msg );
	return true;
}

bool ProtocolJson::MasterJobCompletionPing( std::string &msg, int64_t jobId, int taskId )
{
	msg = std::string( "{\"type\":\"job_completion\"}\n" );
    msg += std::string( "{\"job_id\":\"" ) + boost::lexical_cast<std::string>( jobId ) + "\","
        "\"task_id\":" + boost::lexical_cast<std::string>( taskId ) + "\"}";
	AddHeader( msg );
	return true;
}

bool ProtocolJson::SendScript( std::string &msg, const std::string &scriptLanguage,
						 const std::string &script, int64_t jobId, int taskId )
{
    msg = std::string( "{\"type\":\"exec\"}\n" );
	msg += std::string( "{\"lang\":\"" ) + scriptLanguage + "\",\"script\":\"" + script + "\","
        "\"job_id\":\"" + boost::lexical_cast<std::string>( jobId ) + "\","
        "\"task_id\":\"" + boost::lexical_cast<std::string>( taskId ) + "\"}";
	AddHeader( msg );
	return true;
}

bool ProtocolJson::ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                    std::string &script, int64_t &jobId, int &taskId )
{
	std::stringstream ss;
	ss << msg;

	boost::property_tree::ptree ptree;
	try
	{
		boost::property_tree::read_json( ss, ptree );
		scriptLanguage = ptree.get<std::string>( "lang" );
	    script = ptree.get<std::string>( "script" );
        jobId = ptree.get<int64_t>( "job_id" );
        taskId = ptree.get<int>( "task_id" );
	}
	catch( std::exception &e )
	{
		PS_LOG( "ProtocolJson::ParseSendScript: " << e.what() );
		return false;
	}

	return true;
}

bool ProtocolJson::GetJobResult( std::string &msg )
{
    msg = std::string( "{\"type\":\"get_result\"}\n" );
	AddHeader( msg );
	return true;
}

bool ProtocolJson::ParseMsgType( const std::string &msg, std::string &type )
{
	std::stringstream ss;
	ss << msg;

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

void ProtocolJson::AddHeader( std::string &msg )
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

} // namespace python_server

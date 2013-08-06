#include <sstream>
#include <boost/property_tree/json_parser.hpp>
#include "protocol.h"
#include "log.h"
#include "helper.h"

namespace python_server {


bool ProtocolJson::NodePing( std::string &msg, const std::string &host )
{
	msg = std::string( "{\"type\":\"ping\",\"host\":\"" ) + host + "\"}";
	AddHeader( msg );
	return true;
}

bool ProtocolJson::NodeResponsePing( std::string &msg )
{
	AddHeader( msg );
	return true;
}

bool ProtocolJson::SendScript( std::string &msg, const std::string &scriptLanguage,
						 const std::string &script )
{
	std::string script64;
	EncodeBase64( script.c_str(), script.size(), script64 );
	msg = std::string( "{\"lang\":\"" ) + scriptLanguage + "\",\"script\":\"" + script64 + "\"}";
	AddHeader( msg );
	return true;
}

bool ProtocolJson::ParseSendScript( const std::string &msg, std::string &scriptLanguage,
							  std::string &script )
{
	std::stringstream ss;
	ss << msg;

	boost::property_tree::ptree ptree;
	try
	{
		boost::property_tree::read_json( ss, ptree );
		scriptLanguage = ptree.get<std::string>( "lang" );
		std::string script64 = ptree.get<std::string>( "script" );
		DecodeBase64( script64, script );
	}
	catch( std::exception &e )
	{
		PS_LOG( "ProtocolJson::ParseSendScript: " << e.what() );
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

#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include <sstream>
#include <boost/property_tree/json_parser.hpp>
#include "common/log.h"

namespace python_server {

class Protocol
{
public:
	virtual ~Protocol() {}
	virtual bool NodePing( std::string &msg, const std::string &hostName ) = 0;
	virtual bool NodeResponsePing( std::string &msg ) = 0;
    virtual bool SendScript( std::string &msg, const std::string &scriptLanguage,
                             const std::string &script ) = 0;
    virtual bool ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                  std::string &script ) = 0;
};

class ProtocolJson : public Protocol
{
public:
	virtual bool NodePing( std::string &msg, const std::string &host )
	{
		msg = std::string( "{type: \"ping\",host: \"" ) + host + "\"}";
		AddHeader( msg );
		return true;
	}

    virtual bool NodeResponsePing( std::string &msg )
    {
		AddHeader( msg );
		return true;
    }

    virtual bool SendScript( std::string &msg, const std::string &scriptLanguage,
                             const std::string &script )
    {
        msg = std::string( "{lang: \"" ) + scriptLanguage + "\",script: \"" + script + "\"}";
        AddHeader( msg );
        return true;
    }

    virtual bool ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                  std::string &script )
    {
        std::stringstream ss;
        ss << msg;

        boost::property_tree::ptree ptree;
        try
        {
            boost::property_tree::read_json( ss, ptree );
            scriptLanguage = ptree.get<std::string>( "lang" );
            script = ptree.get<std::string>( "script" );
        }
        catch( std::exception &e )
        {
            PS_LOG( "ProtocolJson::ParseSendScript: " << e.what() );
            return false;
        }

        return true;
    }

private:
	void AddHeader( std::string &msg )
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

	const char *GetProtocolType() const { return "json"; }
	const char *GetProtocolVersion() const { return "1"; }
};

class ProtocolCreator
{
public:
	virtual Protocol *Create( const std::string &protocol, int version )
	{
		if ( protocol == "json" )
			return new ProtocolJson();
		return NULL;
	}
};

} // namespace python_server

#endif

#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include <sstream>

namespace python_server {

class Protocol
{
public:
	virtual ~Protocol() {}
	virtual bool NodePing( std::string &msg, const std::string &hostName ) = 0;
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

} // namespace python_server

#endif

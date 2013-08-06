#ifndef __PROTOCOL_H
#define __PROTOCOL_H

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
	virtual bool NodePing( std::string &msg, const std::string &host );

    virtual bool NodeResponsePing( std::string &msg );

    virtual bool SendScript( std::string &msg, const std::string &scriptLanguage,
                             const std::string &script );

    virtual bool ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                  std::string &script );

private:
	void AddHeader( std::string &msg );

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

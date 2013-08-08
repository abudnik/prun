#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include <string>
#include <stdint.h>

namespace python_server {

class Protocol
{
public:
	virtual ~Protocol() {}
	virtual bool NodePing( std::string &msg, const std::string &hostName ) = 0;
	virtual bool NodeResponsePing( std::string &msg ) = 0;
	virtual bool MasterJobCompletionPing( std::string &msg, int64_t jobId, int taskId ) = 0;
    virtual bool SendScript( std::string &msg, const std::string &scriptLanguage,
                             const std::string &script, int64_t jobId, int taskId ) = 0;
    virtual bool ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                  std::string &script, int64_t &jobId, int &taskId ) = 0;
    virtual bool GetJobResult( std::string &msg ) = 0;
    virtual bool ParseMsgType( const std::string &msg, std::string &type ) = 0;
};

class ProtocolJson : public Protocol
{
public:
	virtual bool NodePing( std::string &msg, const std::string &host );

    virtual bool NodeResponsePing( std::string &msg );

	virtual bool MasterJobCompletionPing( std::string &msg, int64_t jobId, int taskId );

    virtual bool SendScript( std::string &msg, const std::string &scriptLanguage,
                             const std::string &script, int64_t jobId, int taskId );

    virtual bool ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                  std::string &script, int64_t &jobId, int &taskId );

    virtual bool GetJobResult( std::string &msg );

    virtual bool ParseMsgType( const std::string &msg, std::string &type );

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

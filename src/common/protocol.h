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
    virtual bool NodeResponsePing( std::string &msg, int numCPU ) = 0;
    virtual bool ParseResponsePing( const std::string &msg, int &numCPU ) = 0;
    virtual bool NodeJobCompletionPing( std::string &msg, int64_t jobId, int taskId ) = 0;
    virtual bool ParseJobCompletionPing( const std::string &msg, int64_t &jobId, int &taskId ) = 0;

    virtual bool SendScript( std::string &msg, const std::string &scriptLanguage,
                             const std::string &script, int64_t jobId, int taskId, int numTasks,
                             int numCPU, int timeout ) = 0;
    virtual bool ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                  std::string &script, int64_t &jobId, int &taskId, int &numTasks,
                                  int &numCPU, int &timeout ) = 0;
    virtual bool GetJobResult( std::string &msg, int64_t jobId, int taskId ) = 0;
    virtual bool ParseGetJobResult( const std::string &msg, int64_t &jobId, int &taskId ) = 0;
    virtual bool SendJobResult( std::string &msg, int errCode ) = 0;
    virtual bool ParseJobResult( const std::string &msg, int &errCode ) = 0;

    virtual bool ParseMsgType( const std::string &msg, std::string &type ) = 0;

    static bool ParseMsg( const std::string &msg, std::string &protocol, int &version,
                          std::string &header, std::string &body );

protected:
    void AddHeader( std::string &msg );

private:
    virtual const char *GetProtocolType() const = 0;
    virtual const char *GetProtocolVersion() const = 0;
};

class ProtocolJson : public Protocol
{
public:
    virtual bool NodePing( std::string &msg, const std::string &host );

    virtual bool NodeResponsePing( std::string &msg, int numCPU );

    virtual bool ParseResponsePing( const std::string &msg, int &numCPU );

    virtual bool NodeJobCompletionPing( std::string &msg, int64_t jobId, int taskId );

    virtual bool ParseJobCompletionPing( const std::string &msg, int64_t &jobId, int &taskId );

    virtual bool SendScript( std::string &msg, const std::string &scriptLanguage,
                             const std::string &script, int64_t jobId, int taskId, int numTasks,
                             int numCPU, int timeout );

    virtual bool ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                  std::string &script, int64_t &jobId, int &taskId, int &numTasks,
                                  int &numCPU, int &timeout );

    virtual bool GetJobResult( std::string &msg, int64_t jobId, int taskId );

    virtual bool ParseGetJobResult( const std::string &msg, int64_t &jobId, int &taskId );

    virtual bool SendJobResult( std::string &msg, int errCode );

    virtual bool ParseJobResult( const std::string &msg, int &errCode );

    virtual bool ParseMsgType( const std::string &msg, std::string &type );

private:
    virtual const char *GetProtocolType() const { return "json"; }
    virtual const char *GetProtocolVersion() const { return "1"; }
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

#ifndef __JSON_RPC_H
#define __JSON_RPC_H

#include <string>
#include <map>
#include <boost/property_tree/ptree.hpp>
#include <boost/function.hpp>

#define JSON_RPC_PARSER_ERROR     -32700
#define JSON_RPC_INVALID_REQUEST  -32600
#define JSON_RPC_METHOD_NOT_FOUND -32601
#define JSON_RPC_INVALID_PARAMS   -32602
#define JSON_RPC_INTERNAL_ERROR   -32603
//--------------------------------------
#define JSON_RPC_VERSION_MISMATCH -32000

namespace common {

class JsonRpcHandler
{
public:
    virtual ~JsonRpcHandler() {}
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result ) = 0;
};

class JsonRpc
{
private:
    JsonRpc();

public:
    int HandleRequest( const std::string &request, std::string &requestId, std::string &result );

    bool RegisterHandler( const std::string &method, JsonRpcHandler *handler );

    void Shutdown();

    static JsonRpc &Instance()
    {
        static JsonRpc instance_;
        return instance_;
    }

    static bool ValidateJsonBraces( const std::string &json );
    bool GetErrorDescription( int errCode, std::string &descr ) const;

    static const char *GetProtocolVersion() { return "2.0"; }

private:
    JsonRpcHandler *GetHandler( const std::string &method ) const;

private:
    std::map< std::string, JsonRpcHandler * > cmdToHandler_;
    std::map< int, std::string > errDescription_;
};

} // namespace common

#endif

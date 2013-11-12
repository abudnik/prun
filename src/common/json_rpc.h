#ifndef __JSON_RPC_H
#define __JSON_RPC_H

#include <string>
#include <map>
#include <boost/property_tree/ptree.hpp>
#include <boost/function.hpp>

namespace common {

typedef boost::function< void (const std::string &method, const boost::property_tree::ptree &params) > JsonRpcCallback;

class JsonRpcCaller
{
public:
    virtual ~JsonRpcCaller() {}
    virtual void RpcCall( const std::string &method, const boost::property_tree::ptree &params ) = 0;
};

class JsonRpcHandler
{
public:
    virtual ~JsonRpcHandler() {}
    virtual int Execute( const boost::property_tree::ptree &ptree,
                         JsonRpcCaller *caller ) = 0;
};

class JsonRpc
{
private:
    JsonRpc();

public:
    int HandleRequest( const std::string &request, JsonRpcCaller *caller );

    bool HandleResponse( const std::string &response );

    bool RegisterHandler( const std::string &command, JsonRpcHandler *handler );

    void Shutdown();

    static JsonRpc &Instance()
    {
        static JsonRpc instance_;
        return instance_;
    }

    static bool ValidateJsonBraces( const std::string &json );
    bool GetErrorDescription( int errCode, std::string &descr ) const;

private:
    const char *GetProtocolVersion() const { return "2.0"; }

private:
    std::map< std::string, JsonRpcHandler * > cmdToHandler_;
    std::map< int, std::string > errDescription_;
};

} // namespace common

#endif

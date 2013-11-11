#ifndef __JSON_RPC_H
#define __JSON_RPC_H

#include <string>
#include <boost/property_tree/ptree.hpp>

namespace common {

class JsonRpcHandler
{
public:
    virtual ~AdminCommand() {}
    virtual void Execute( const std::string &command,
                          const boost::property_tree::ptree &ptree ) = 0;
};

class JsonRpc
{
public:
    bool HandleRequest( const std::string &request );
    bool HandleResponse( const std::string &response );

    bool RegisterHandler( JsonRpcHandler *handler );

private:
    const char *GetProtocolVersion() const { return "2.0"; }

private:
};

} // namespace common

#endif

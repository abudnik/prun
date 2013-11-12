#define BOOST_SPIRIT_THREADSAFE

#include "json_rpc.h"
#include "log.h"

namespace common {

bool JsonRpc::HandleRequest( const std::string &request, JsonRpcCaller *caller )
{
    return true;
}

bool JsonRpc::RegisterHandler( const std::string &command, JsonRpcHandler *handler )
{
    std::map< std::string, JsonRpcHandler * >::const_iterator it;
    it = cmdToHandler_.find( command );
    if ( it != cmdToHandler_.end() )
    {
        PS_LOG( "JsonRpc::RegisterHandler: handler for command '" << command <<
                "' already exists" );
        return false;
    }

    cmdToHandler_[ command ] = handler;
    return true;
}

void JsonRpc::Shutdown()
{
    std::map< std::string, JsonRpcHandler * >::iterator it = cmdToHandler_.begin();
    for( ; it != cmdToHandler_.end(); ++it )
    {
        delete it->second;
    }
}

bool JsonRpc::ValidateJsonBraces( const std::string &json )
{
    int c = 0;
    for( size_t i = 0; i < json.size(); ++i )
    {
        c = json[i];
        if ( c == '{' )
        {
            ++c;
        }
        if ( c == '}' )
        {
            if ( c == 0 )
                return false;
            else
                --c;
        }
    }
    return c == 0;
}

} // namespace common

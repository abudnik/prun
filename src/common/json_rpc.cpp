#define BOOST_SPIRIT_THREADSAFE

#include <boost/property_tree/json_parser.hpp>
#include "json_rpc.h"
#include "log.h"

#define JSON_RPC_PARSER_ERROR     -32700
#define JSON_RPC_INVALID_REQUEST  -32600
#define JSON_RPC_METHOD_NOT_FOUND -32601
#define JSON_RPC_INVALID_PARAMS   -32602
#define JSON_RPC_INTERNAL_ERROR   -32603

namespace common {

JsonRpc::JsonRpc()
{
    errDescription_[ JSON_RPC_PARSER_ERROR ] = "Parse error";
    errDescription_[ JSON_RPC_INVALID_REQUEST ] = "Invalid Request";
    errDescription_[ JSON_RPC_METHOD_NOT_FOUND ] = "Method not found";
    errDescription_[ JSON_RPC_INVALID_PARAMS ] = "Invalid params";
    errDescription_[ JSON_RPC_INTERNAL_ERROR ] = "Internal error";
}

int JsonRpc::HandleRequest( const std::string &request, JsonRpcCaller *caller )
{
    std::istringstream ss( request );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
    }
    catch( std::exception &e )
    {
        PS_LOG( "ProtocolJson::ParseJobCompletionPing: " << e.what() );
        return JSON_RPC_PARSER_ERROR;
    }

    return 0;
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
    int b = 0;
    for( size_t i = 0; i < json.size(); ++i )
    {
        char c = json[i];
        if ( c == '{' )
        {
            ++b;
        }
        if ( c == '}' )
        {
            if ( b == 0 )
                return false;
            else
                --b;
        }
    }
    return b == 0;
}

bool JsonRpc::GetErrorDescription( int errCode, std::string &descr ) const
{
    if ( errCode >= -32000 && errCode < 32100 )
    {
        descr = "Server error";
        return true;
    }

    std::map< int, std::string >::const_iterator it = errDescription_.find( errCode );
    if ( it != errDescription_.end() )
    {
        descr = it->second;
        return true;
    }
    return false;
}

} // namespace common

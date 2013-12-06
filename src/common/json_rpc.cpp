#define BOOST_SPIRIT_THREADSAFE

#include <boost/property_tree/json_parser.hpp>
#include "json_rpc.h"
#include "log.h"

namespace common {

JsonRpc::JsonRpc()
{
    errDescription_[ JSON_RPC_PARSER_ERROR ] = "Parse error";
    errDescription_[ JSON_RPC_INVALID_REQUEST ] = "Invalid Request";
    errDescription_[ JSON_RPC_METHOD_NOT_FOUND ] = "Method not found";
    errDescription_[ JSON_RPC_INVALID_PARAMS ] = "Invalid params";
    errDescription_[ JSON_RPC_INTERNAL_ERROR ] = "Internal error";
    errDescription_[ JSON_RPC_VERSION_MISMATCH ] = "Json-rpc version mismatch";
}

int JsonRpc::HandleRequest( const std::string &request, std::string &requestId, std::string &result )
{
    boost::property_tree::ptree ptree;
    try
    {
        std::istringstream ss( request );
        boost::property_tree::read_json( ss, ptree );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "JsonRpc::HandleRequest: " << e.what() );
        return JSON_RPC_PARSER_ERROR;
    }

    try
    {
        std::string version = ptree.get<std::string>( "jsonrpc" );
        if ( version != GetProtocolVersion() )
            return JSON_RPC_VERSION_MISMATCH;

        requestId = ptree.get<std::string>( "id" );

        std::string method = ptree.get<std::string>( "method" );
        JsonRpcHandler *handler = GetHandler( method );
        if ( !handler )
            return JSON_RPC_METHOD_NOT_FOUND;

        const boost::property_tree::ptree &params = ptree.get_child( "params" );
        return handler->Execute( params, result );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "JsonRpc::HandleRequest: " << e.what() );
        return JSON_RPC_INTERNAL_ERROR;
    }

    return 0;
}

bool JsonRpc::RegisterHandler( const std::string &method, JsonRpcHandler *handler )
{
    std::map< std::string, JsonRpcHandler * >::const_iterator it;
    it = cmdToHandler_.find( method );
    if ( it != cmdToHandler_.end() )
    {
        PLOG_ERR( "JsonRpc::RegisterHandler: handler for method '" << method <<
                  "' already exists" );
        return false;
    }

    cmdToHandler_[ method ] = handler;
    return true;
}

JsonRpcHandler *JsonRpc::GetHandler( const std::string &method ) const
{
    std::map< std::string, JsonRpcHandler * >::const_iterator it;
    it = cmdToHandler_.find( method );
    if ( it != cmdToHandler_.end() )
        return it->second;
    return NULL;
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
    std::map< int, std::string >::const_iterator it = errDescription_.find( errCode );
    if ( it != errDescription_.end() )
    {
        descr = it->second;
        return true;
    }

    if ( errCode >= -32000 && errCode < 32100 )
    {
        descr = "Server error";
        return true;
    }

    return false;
}

} // namespace common

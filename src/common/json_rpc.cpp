/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2013 Andrey Budnik <budnik27@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

===========================================================================
*/

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

JsonRpc::~JsonRpc()
{
    std::map< std::string, IJsonRpcHandler * >::iterator it = cmdToHandler_.begin();
    for( ; it != cmdToHandler_.end(); ++it )
    {
        delete it->second;
    }
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
        IJsonRpcHandler *handler = GetHandler( method );
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

bool JsonRpc::RegisterHandler( const std::string &method, IJsonRpcHandler *handler )
{
    std::map< std::string, IJsonRpcHandler * >::const_iterator it;
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

IJsonRpcHandler *JsonRpc::GetHandler( const std::string &method ) const
{
    std::map< std::string, IJsonRpcHandler * >::const_iterator it;
    it = cmdToHandler_.find( method );
    if ( it != cmdToHandler_.end() )
        return it->second;
    return NULL;
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

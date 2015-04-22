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

#ifndef __JSON_RPC_H
#define __JSON_RPC_H

#include <string>
#include <map>
#include <boost/property_tree/ptree.hpp>

#define JSON_RPC_PARSER_ERROR     -32700
#define JSON_RPC_INVALID_REQUEST  -32600
#define JSON_RPC_METHOD_NOT_FOUND -32601
#define JSON_RPC_INVALID_PARAMS   -32602
#define JSON_RPC_INTERNAL_ERROR   -32603
//--------------------------------------
#define JSON_RPC_VERSION_MISMATCH -32000

namespace common {

struct IJsonRpcHandler
{
    virtual ~IJsonRpcHandler() {}
    virtual int Execute( const boost::property_tree::ptree &params,
                         std::string &result ) = 0;
};

class JsonRpc
{
public:
    JsonRpc();
    ~JsonRpc();

    int HandleRequest( const std::string &request, std::string &requestId, std::string &result );

    bool RegisterHandler( const std::string &method, IJsonRpcHandler *handler );

    bool GetErrorDescription( int errCode, std::string &descr ) const;

    static bool ValidateJsonBraces( const std::string &json );

    static const char *GetProtocolVersion() { return "2.0"; }

private:
    IJsonRpcHandler *GetHandler( const std::string &method ) const;

private:
    std::map< std::string, IJsonRpcHandler * > cmdToHandler_;
    std::map< int, std::string > errDescription_;
};

} // namespace common

#endif

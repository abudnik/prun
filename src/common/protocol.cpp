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

#include <sstream>
#include <stdint.h> // boost/atomic/atomic.hpp:202:16: error: ‘uintptr_t’ was not declared in this scope
#include <boost/property_tree/json_parser.hpp>
#include "protocol.h"

namespace common {


std::string Protocol::CreateHeader( const std::string &msg ) const
{
    std::string header( GetProtocolType() );
    header += '\n';
    header += GetProtocolVersion();
    header += '\n';

    const size_t size = header.size() + msg.size();
    return std::to_string( size ) + '\n' + header;
}

bool Protocol::ParseMsg( const std::string &msg, std::string &protocol, int &version,
                         std::string &header, std::string &body )
{
    int size;
    try
    {
        std::istringstream ss( msg );
        ss >> size >> protocol >> version >> header >> body;
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::ParseMsg: " << e.what() );
        return false;
    }
    return true;
}


bool ProtocolJson::Serialize( std::string &msg, const char *method, const Marshaller &marshaller )
{
    std::ostringstream ss;
    try
    {
        boost::property_tree::write_json( ss, marshaller.GetProperties(), false );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::Serialize: " << e.what() );
        return false;
    }
    msg = std::string( "{\"type\":\"" ) + std::string( method ) + std::string( "\"}\n" );
    msg += ss.str();
    msg = CreateHeader( msg ) + msg;
    return true;
}

bool ProtocolJson::ParseBody( const std::string &msg, Demarshaller::Properties &ptree )
{
    try
    {
        std::istringstream ss( msg );
        boost::property_tree::read_json( ss, ptree );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::ParseBody: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::SendCommand( std::string &msg, const std::string &masterId, const std::string &command,
                                const std::list< std::pair< std::string, std::string > > &params )
{
    std::ostringstream ss;
    boost::property_tree::ptree ptree;
    try
    {
        ptree.put( "master_id", masterId );

        for( const auto &param : params )
        {
            ptree.put( param.first, param.second );
        }
        boost::property_tree::write_json( ss, ptree, false );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::SendCommand: " << e.what() );
        return false;
    }
    msg = std::string( "{\"type\":\"" ) + command + std::string( "\"}\n" );
    msg += ss.str();
    msg = CreateHeader( msg ) + msg;
    return true;
}

bool ProtocolJson::ParseMsgType( const std::string &msg, std::string &type )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        type = ptree.get<std::string>( "type" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::ParseMsgType: " << e.what() );
        return false;
    }

    return true;
}

} // namespace common

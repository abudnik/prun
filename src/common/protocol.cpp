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
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>
#include "protocol.h"

namespace common {


void Protocol::AddHeader( std::string &msg )
{
    std::string header( GetProtocolType() );
    header += '\n';
    header += GetProtocolVersion();
    header += '\n';

    size_t size = header.size() + msg.size();
    std::ostringstream ss;
    ss << size << '\n' << header;
    msg.insert( 0, ss.str() );
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
    AddHeader( msg );
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

bool ProtocolJson::ParseJobCompletionPing( const std::string &msg, int64_t &jobId, int &taskId )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        jobId = ptree.get<int64_t>( "job_id" );
        taskId = ptree.get<int>( "task_id" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::ParseJobCompletionPing: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                    std::string &script, std::string &filePath,
                                    std::string &masterId,
                                    int64_t &jobId, std::set<int> &tasks,
                                    int &numTasks, int &timeout )
{
    std::istringstream ss( msg );
    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        scriptLanguage = ptree.get<std::string>( "lang" );
        script = ptree.get<std::string>( "script" );
        filePath = ptree.get<std::string>( "file_path" );
        masterId = ptree.get<std::string>( "master_id" );
        jobId = ptree.get<int64_t>( "job_id" );
        numTasks = ptree.get<int>( "num_tasks" );
        timeout = ptree.get<int>( "timeout" );

        BOOST_FOREACH( const boost::property_tree::ptree::value_type &v,
                       ptree.get_child( "tasks" ) )
        {
            tasks.insert( v.second.get_value< int >() );
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::ParseSendScript: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::ParseGetJobResult( const std::string &msg, std::string &masterId, int64_t &jobId, int &taskId )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        masterId = ptree.get<std::string>( "master_id" );
        jobId = ptree.get<int64_t>( "job_id" );
        taskId = ptree.get<int>( "task_id" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::ParseGetJobResult: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::ParseJobResult( const std::string &msg, int &errCode, int64_t &execTime )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        errCode = ptree.get<int>( "err_code" );
        execTime = ptree.get<int64_t>( "elapsed" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::ParseJobResult: " << e.what() );
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

        std::list< std::pair< std::string, std::string > >::const_iterator it = params.begin();
        for( ; it != params.end(); ++it )
        {
            ptree.put( it->first, it->second );
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
    AddHeader( msg );
    return true;
}

bool ProtocolJson::ParseSendCommandResult( const std::string &msg, int &errCode )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        errCode = ptree.get<int>( "err_code" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::ParseSendCommandResult: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::ParseStopTask( const std::string &msg, std::string &masterId, int64_t &jobId, int &taskId )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        masterId = ptree.get<std::string>( "master_id" );
        jobId = ptree.get<int64_t>( "job_id" );
        taskId = ptree.get<int>( "task_id" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::ParseStopTask: " << e.what() );
        return false;
    }

    return true;
}

bool ProtocolJson::ParseStopPreviousJobs( const std::string &msg, std::string &masterId )
{
    std::istringstream ss( msg );

    boost::property_tree::ptree ptree;
    try
    {
        boost::property_tree::read_json( ss, ptree );
        masterId = ptree.get<std::string>( "master_id" );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ProtocolJson::ParseStopPreviousJobs: " << e.what() );
        return false;
    }

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

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

#include <boost/lexical_cast.hpp>
#include "dbconnection.h"
#include "common/config.h"
#include "common/log.h"

namespace master {

void DbHistoryConnection::Send( const std::string &request, Callback &callback )
{
    if ( !established_ )
        return;
}

bool DbHistoryConnection::Connect( const std::string &host, unsigned short port )
{
    try
    {
        boost::system::error_code ec;
        tcp::resolver resolver( io_service_ );

        common::Config &cfg = common::Config::Instance();
        bool ipv6 = cfg.Get<bool>( "ipv6" );

        std::string sPort = boost::lexical_cast<std::string>( port );

        tcp::resolver::query query( ipv6 ? tcp::v6() : tcp::v4(), host, sPort );
        tcp::resolver::iterator iterator = resolver.resolve( query, ec ), end;
        if ( ec || iterator == end )
        {
            PLOG_WRN( "DbHistoryConnection::Connect: address not resolved: '" << host << " : " << sPort << "'" );
            return false;
        }

        boost::asio::connect( socket_, iterator, ec );
        if ( ec )
        {
            PLOG_WRN( "DbHistoryConnection::Connect: couldn't connect to '" << host << " : " << sPort << "': " << ec.message() );
            return false;
        }
    }
    catch( std::exception &e )
    {
        PLOG_WRN( "DbHistoryConnection::Connect exception: " << e.what() );
        return false;
    }

    return true;
}

} // namespace master

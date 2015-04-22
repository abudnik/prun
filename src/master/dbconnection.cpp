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

#include "dbconnection.h"
#include "common/config.h"
#include "common/log.h"

namespace master {

void DbHistoryConnection::Send( const std::string &request, Callback &callback )
{
    if ( !established_ )
        return;

    std::unique_lock< std::mutex > lock( mut_ );

    try
    {
        boost::asio::write( socket_, boost::asio::buffer( request ), boost::asio::transfer_all() );

        boost::system::error_code error;
        bool firstRead = true;
        while( true )
        {
            size_t bytes_transferred = socket_.read_some( boost::asio::buffer( buffer_ ), error );
            if ( !bytes_transferred )
            {
                PLOG_ERR( "DbHistoryConnection::Send: read_some failed, error=" << error.message() );
                break;
            }

            if ( firstRead )
            {
                int ret = response_.OnFirstRead( buffer_, bytes_transferred );
                firstRead = ( ret == 0 );
            }
            if ( !firstRead )
            {
                response_.OnRead( buffer_, bytes_transferred );

                if ( response_.IsReadCompleted() )
                {
                    callback( response_.GetString() );
                    break;
                }
            }
        }
    }
    catch( boost::system::system_error &e )
    {
        PLOG_ERR( "DbHistoryConnection::Send() failed: " << e.what() );
        established_ = false;
    }

    response_.Reset();
}

bool DbHistoryConnection::Connect( const std::string &host, unsigned short port )
{
    try
    {
        boost::system::error_code ec;
        tcp::resolver resolver( io_service_ );

        common::Config &cfg = common::Config::Instance();
        bool ipv6 = cfg.Get<bool>( "ipv6" );

        std::string sPort = std::to_string( port );

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

    established_ = true;
    return true;
}

void DbHistoryConnection::Shutdown()
{
    if ( established_ )
    {
        established_ = false;
        boost::system::error_code error;
        socket_.shutdown( boost::asio::ip::tcp::socket::shutdown_both, error );
        error.clear();
        socket_.close( error );
    }
}

} // namespace master

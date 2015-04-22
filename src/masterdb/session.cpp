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

#include <boost/bind.hpp>
#include "session.h"
#include "common/protocol.h"
#include "common/config.h"

namespace masterdb {

void BoostSession::Start()
{
    while( ReadRequest() )
    {
        bool result = HandleRequest( request_ );

        if ( !WriteResponse( result ) )
            break;

        request_.Reset();
        response_.clear();
    }
}

void BoostSession::Stop()
{
    boost::system::error_code error;
    socket_.shutdown( boost::asio::ip::tcp::socket::shutdown_both, error );
    error.clear();
    socket_.close( error );
}

bool BoostSession::ReadRequest()
{
    try
    {
        boost::system::error_code error;
        bool firstRead = true;
        while( true )
        {
            size_t bytes_transferred = socket_.read_some( boost::asio::buffer( buffer_ ), error );
            if ( !bytes_transferred )
            {
                PLOG_ERR( "BoostSession::ReadRequest: read_some failed, error=" << error.message() );
                return false;
            }

            if ( firstRead )
            {
                int ret = request_.OnFirstRead( buffer_, bytes_transferred );
                firstRead = ( ret == 0 );
            }
            if ( !firstRead )
            {
                request_.OnRead( buffer_, bytes_transferred );
                if ( request_.IsReadCompleted() )
                    break;
            }
        }
    }
    catch( boost::system::system_error &e )
    {
        PLOG_ERR( "BoostSession::ReadRequest() failed: " << e.what() );
        return false;
    }

    return true;
}

bool BoostSession::WriteResponse( bool result )
{
    if ( response_.empty() )
    {
        response_ = result ? '1' : '0';
    }
    response_.insert( 0, std::to_string( response_.size() ) + '\n' );

    try
    {
        boost::asio::write( socket_, boost::asio::buffer( response_ ), boost::asio::transfer_all() );
    }
    catch( boost::system::system_error &e )
    {
        PLOG_ERR( "BoostSession::WriteResponse() failed: " << e.what() );
        return false;
    }

    return true;
}


ConnectionAcceptor::ConnectionAcceptor( boost::asio::io_service &io_service, unsigned short port )
: io_service_( io_service ),
 acceptor_( io_service )
{
    try
    {
        common::Config &cfg = common::Config::Instance();
        bool ipv6 = cfg.Get<bool>( "ipv6" );

        tcp::endpoint endpoint( ipv6 ? tcp::v6() : tcp::v4(), port );
        acceptor_.open( endpoint.protocol() );
        acceptor_.set_option( tcp::acceptor::reuse_address( true ) );
        acceptor_.set_option( tcp::no_delay( true ) );
        acceptor_.bind( endpoint );
        acceptor_.listen();
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ConnectionAcceptor: " << e.what() );
    }

    StartAccept();
}

void ConnectionAcceptor::StartAccept()
{
    session_.reset( new BoostSession( io_service_ ) );
    acceptor_.async_accept( session_->GetSocket(),
                            boost::bind( &ConnectionAcceptor::HandleAccept, this,
                                         boost::asio::placeholders::error ) );
}

void ConnectionAcceptor::Stop()
{
    if ( session_ )
    {
        session_->Stop();
    }
}

void ConnectionAcceptor::HandleAccept( const boost::system::error_code &error )
{
    if ( !error )
    {
        PLOG( "connection accepted..." );
        session_->Start();
        PLOG( "connection lost..." );
        StartAccept();
    }
    else
    {
        PLOG_ERR( "HandleAccept: " << error.message() );
    }
}

} // namespace masterdb

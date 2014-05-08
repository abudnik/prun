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

#ifndef __DB_SESSION_H
#define __DB_SESSION_H

#include <boost/asio.hpp>
#include "common/log.h"
#include "common/request.h"
#include "dbaction.h"

using boost::asio::ip::tcp;

namespace masterdb {

class Session
{
public:
    virtual ~Session() {}

protected:
    template< typename T >
    bool HandleRequest( T &request )
    {
        dbrequest_.Reset();

        const std::string &req = request.GetString();
        if ( dbrequest_.ParseRequest( req ) )
        {
            PLOG( dbrequest_.GetType() );
            PLOG( dbrequest_.GetArgs().size() );
            PLOG( dbrequest_.GetData() );

            DbActionCreator actionCreator;
            boost::scoped_ptr< DbAction > action(
                actionCreator.Create( dbrequest_.GetType() )
            );
            if ( action )
            {
                return action->Execute( dbrequest_ );
            }
            else
            {
                PLOG_WRN( "Session::HandleRequest: appropriate action not found for task type: "
                          << dbrequest_.GetType() );
            }
        }
        else
        {
            PLOG_ERR( req );
        }

        return false;
    }

private:
    DbRequest dbrequest_;
};

class BoostSession : public Session
{
public:
    typedef boost::array< char, 32 * 1024 > BufferType;

public:
    BoostSession( boost::asio::io_service &io_service )
    : socket_( io_service ),
     request_( false )
    {}

    void Start();

    tcp::socket &GetSocket() { return socket_; }

private:
    bool ReadRequest();
    bool WriteResponse();

protected:
    tcp::socket socket_;
    BufferType buffer_;
    common::Request< BufferType > request_;
    char readStatus_;
    std::string response_;
};

class ConnectionAcceptor
{
    typedef boost::shared_ptr< BoostSession > session_ptr;

public:
    ConnectionAcceptor( boost::asio::io_service &io_service, unsigned short port );

private:
    void StartAccept();

    bool HandleAccept( const boost::system::error_code &error );

private:
    boost::asio::io_service &io_service_;
    tcp::acceptor acceptor_;
    session_ptr session_;
};

} // namespace masterdb

#endif
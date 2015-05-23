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

#ifndef __DB_CONNECTION_H
#define __DB_CONNECTION_H

#include <boost/asio.hpp>
#include <boost/array.hpp>
#include <mutex>
#include "common/request.h"

using boost::asio::ip::tcp;


namespace master {

struct IHistoryChannel
{
    typedef std::function< void (const std::string &response) > Callback;

    virtual void Send( const std::string &request, Callback &callback ) = 0;
};

class DbHistoryConnection: public IHistoryChannel
{
    typedef boost::array< char, 2048 > BufferType;

public:
    DbHistoryConnection( boost::asio::io_service &io_service )
    : io_service_( io_service ), socket_( io_service ),
     established_( false ), response_( true )
    {
        buffer_.fill( 0 );
    }

    // IHistoryChannel
    virtual void Send( const std::string &request, Callback &callback );

    bool Connect( const std::string &host, unsigned short port );
    void Shutdown();

private:
    boost::asio::io_service &io_service_;
    tcp::socket socket_;
    bool established_;
    common::Request< BufferType > response_;
    BufferType buffer_;
    std::mutex mut_;
};

} // namespace master

#endif

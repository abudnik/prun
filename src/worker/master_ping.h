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

#ifndef __MASTER_PING_H
#define __MASTER_PING_H

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include "common/protocol.h"
#include "common/config.h"

namespace worker {

class MasterPing
{
public:
    MasterPing()
    {
        protocol_ = new common::ProtocolJson;
    }

    virtual ~MasterPing()
    {
        delete protocol_;
    }

    virtual void Start() = 0;

protected:
    common::Protocol *protocol_;
};

using boost::asio::ip::udp;

class MasterPingBoost : public MasterPing
{
    typedef boost::array< char, 32 * 1024 > BufferType;

public:
    MasterPingBoost( boost::asio::io_service &io_service )
    : socket_( io_service )
    {
        const common::Config &cfg = common::Config::Instance();
        const bool ipv6_only = cfg.Get<bool>( "ipv6_only" );
        const unsigned short port = cfg.Get<unsigned short>( "ping_port" );
        master_ping_port_ = cfg.Get<unsigned short>( "master_ping_port" );

        socket_.open( ipv6_only ? udp::v6() : udp::v4() );
        socket_.bind( udp::endpoint( ipv6_only ? udp::v6() : udp::v4(), port ) );
    }

    virtual void Start();

private:
    void StartReceive();
    void HandleRead( const boost::system::error_code& error, size_t bytes_transferred );

private:
    BufferType buffer_;
    udp::socket socket_;
    udp::endpoint remote_endpoint_;
    unsigned short master_ping_port_;
};

} // namespace worker

#endif

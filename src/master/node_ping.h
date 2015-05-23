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

#ifndef __NODE_PING_H
#define __NODE_PING_H

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include "common/config.h"

namespace master {

class PingReceiver
{
public:
    virtual ~PingReceiver() {}

    virtual void Start() = 0;

protected:
    void OnNodePing( const std::string &nodeIP, const std::string &msg );
};

using boost::asio::ip::udp;

class PingReceiverBoost : public PingReceiver
{
    typedef boost::array< char, 32 * 1024 > BufferType;

public:
    PingReceiverBoost( boost::asio::io_service &io_service )
    : socket_( io_service )
    {
        const common::Config &cfg = common::Config::Instance();
        const bool ipv6 = cfg.Get<bool>( "ipv6" );
        const unsigned short master_ping_port = cfg.Get<unsigned short>( "master_ping_port" );

        socket_.open( ipv6 ? udp::v6() : udp::v4() );
        socket_.bind( udp::endpoint( ipv6 ? udp::v6() : udp::v4(), master_ping_port ) );

        memset( buffer_.c_array(), 0, buffer_.size() );
    }

    virtual void Start();

private:
    void StartReceive();
    void HandleRead( const boost::system::error_code& error, size_t bytes_transferred );

private:
    BufferType buffer_;
    udp::socket socket_;
    udp::endpoint remote_endpoint_;
};

} // namespace master

#endif

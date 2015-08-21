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

#ifndef __PING_H
#define __PING_H

#include <boost/asio.hpp>
#include <sstream>
#include "common/helper.h"
#include "common/protocol.h"
#include "common/config.h"
#include "worker.h"

namespace master {

class Pinger
{
public:
    Pinger( int pingDelay, int maxDroped )
    : stopped_( false ),
     pingDelay_( pingDelay ), maxDroped_( maxDroped ),
     numPings_( 0 )
    {
        protocol_ = new common::ProtocolJson;
    }

    virtual ~Pinger()
    {
        delete protocol_;
    }

    virtual void StartPing() = 0;

    void Stop();

    void Run();

protected:
    void PingWorkers();
    virtual void PingWorker( WorkerPtr &worker ) = 0;

    void CheckDropedPingResponses();

    void OnWorkerIPResolve( WorkerPtr &worker, const std::string &ip );

protected:
    bool stopped_;
    common::SyncTimer timer_;
    int pingDelay_;
    int maxDroped_;
    int numPings_;
    common::Protocol *protocol_;
};


using boost::asio::ip::udp;

class PingerBoost : public Pinger
{
    typedef std::map< std::string, udp::endpoint > EndpointMap;

public:
    PingerBoost( boost::asio::io_service &io_service, int pingDelay, int maxDroped )
    : Pinger( pingDelay, maxDroped ),
     io_service_( io_service ),
     socket_( io_service ),
     resolver_( io_service )
    {
        const common::Config &cfg = common::Config::Instance();
        const bool ipv6_only = cfg.Get<bool>( "ipv6_only" );
        const unsigned short node_ping_port = cfg.Get<unsigned short>( "node_ping_port" );

        socket_.open( ipv6_only ? udp::v6() : udp::v4() );

        std::ostringstream ss;
        ss << node_ping_port;
        port_ = ss.str();
    }

    virtual void StartPing();

private:
    virtual void PingWorker( WorkerPtr &worker );

private:
    boost::asio::io_service &io_service_;
    udp::socket socket_;
    udp::resolver resolver_;
    std::string port_;
    EndpointMap endpoints_;
};

} // namespace master

#endif

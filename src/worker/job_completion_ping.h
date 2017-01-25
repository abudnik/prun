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

#ifndef __JOB_COMPLETION_PING_H
#define __JOB_COMPLETION_PING_H

#include <boost/asio.hpp>
#include <sstream>
#include "common/helper.h"
#include "common/protocol.h"
#include "common/config.h"
#include "job_completion_table.h"

namespace worker {

class JobCompletionPinger
{
public:
    JobCompletionPinger( int pingDelay )
    : stopped_( false ), pingDelay_( pingDelay )
    {
        protocol_ = new common::ProtocolJson;
    }

    virtual ~JobCompletionPinger()
    {
        delete protocol_;
    }

    virtual void StartPing() = 0;

    void Stop();

    void Run();

protected:
    void PingMasters();
    virtual void PingMaster( const JobDescriptor &descr ) = 0;

protected:
    bool stopped_;
    common::SyncTimer timer_;
    int pingDelay_;
    common::Protocol *protocol_;
};


using boost::asio::ip::udp;

class JobCompletionPingerBoost : public JobCompletionPinger
{
public:
    JobCompletionPingerBoost( boost::asio::io_service &io_service, int pingDelay )
    : JobCompletionPinger( pingDelay ),
     io_service_( io_service ),
     socket_( io_service )
    {
        const common::Config &cfg = common::Config::Instance();
        const bool ipv6_only = cfg.Get<bool>( "ipv6_only" );
        master_ping_port_ = cfg.Get<unsigned short>( "master_ping_port" );
        socket_.open( ipv6_only ? udp::v6() : udp::v4() );
    }

    virtual void StartPing();

private:
    virtual void PingMaster( const JobDescriptor &descr );

private:
    boost::asio::io_service &io_service_;
    udp::socket socket_;
    unsigned short master_ping_port_;
};

} // namespace worker

#endif

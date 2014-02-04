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
#include "master_ping.h"
#include "common/log.h"
#include "computer_info.h"

namespace worker {

void MasterPingBoost::Start()
{
    StartReceive();
}

void MasterPingBoost::StartReceive()
{
    socket_.async_receive_from(
        boost::asio::buffer( buffer_ ), remote_endpoint_,
        boost::bind( &MasterPingBoost::HandleRead, this,
                     boost::asio::placeholders::error,
                     boost::asio::placeholders::bytes_transferred ) );
}

void MasterPingBoost::HandleRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( !error )
    {
        //std::string request( buffer_.begin(), buffer_.begin() + bytes_transferred );
        //PLOG( request );

        ComputerInfo &compInfo = ComputerInfo::Instance();
        const int numCPU = compInfo.GetNumCPU();
        const int64_t memSizeMb = compInfo.GetPhysicalMemory() >> 10; // divide by 1024

        common::Marshaller marshaller;
        marshaller( "num_cpu", numCPU )
                  ( "mem_size", memSizeMb );

        std::string msg;
        protocol_->Serialize( msg, "ping_response", marshaller );

        udp::endpoint master_endpoint( remote_endpoint_.address(), DEFAULT_MASTER_UDP_PORT );

        try
        {
            socket_.send_to( boost::asio::buffer( msg ), master_endpoint );
        }
        catch( boost::system::system_error &e )
        {
            PLOG_ERR( "MasterPingBoost::HandleRead: send_to failed: " << e.what() << ", host : " << remote_endpoint_ );
        }
    }
    else
    {
        PLOG_ERR( "MasterPingBoost::HandleRead error=" << error );
    }

    StartReceive();
}

} // namespace worker


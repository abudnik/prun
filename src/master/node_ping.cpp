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
#include <boost/scoped_ptr.hpp>
#include "node_ping.h"
#include "common/log.h"
#include "common/protocol.h"
#include "worker_manager.h"

namespace master {

void PingReceiver::OnNodePing( const std::string &nodeIP, const std::string &msg )
{
    //PLOG( nodeIP << " : " << msg );

    std::string protocol, header, body;
    int version;
    if ( !common::Protocol::ParseMsg( msg, protocol, version, header, body ) )
    {
        PLOG_ERR( "PingReceiver::OnNodePing: couldn't parse msg: " << msg );
        return;
    }

    common::ProtocolCreator protocolCreator;
    boost::scoped_ptr< common::Protocol > parser(
        protocolCreator.Create( protocol, version )
    );
    if ( !parser )
    {
        PLOG_ERR( "PingReceiver::OnNodePing: appropriate parser not found for protocol: "
                  << protocol << " " << version );
        return;
    }

    std::string type;
    parser->ParseMsgType( header, type );
    if ( !parser->ParseMsgType( header, type ) )
    {
        PLOG_ERR( "PingReceiver::OnNodePing: couldn't parse msg type: " << header );
        return;
    }

    if ( type == "ping_response" )
    {
        int numCPU;
        int64_t memSizeMb;
        if ( parser->ParseResponsePing( body, numCPU, memSizeMb ) )
        {
            WorkerManager::Instance().OnNodePingResponse( nodeIP, numCPU, memSizeMb );
        }
        else
        {
            PLOG_ERR( "PingReceiver::OnNodePing: couldn't parse msg body: " << body );
        }
    }
    if ( type == "job_completion" )
    {
        int64_t jobId;
        int taskId;
        if ( parser->ParseJobCompletionPing( body, jobId, taskId ) )
        {
            WorkerManager::Instance().OnNodeTaskCompletion( nodeIP, jobId, taskId );
        }
        else
        {
            PLOG_ERR( "PingReceiver::OnNodePing: couldn't parse msg body: " << body );
        }
    }
}

void PingReceiverBoost::Start()
{
    StartReceive();
}

void PingReceiverBoost::StartReceive()
{
    socket_.async_receive_from(
        boost::asio::buffer( buffer_ ), remote_endpoint_,
        boost::bind( &PingReceiverBoost::HandleRead, this,
                     boost::asio::placeholders::error,
                     boost::asio::placeholders::bytes_transferred ) );
}

void PingReceiverBoost::HandleRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( !error )
    {
        std::string response( buffer_.begin(), buffer_.begin() + bytes_transferred );
        std::string nodeIP( remote_endpoint_.address().to_string() );
        StartReceive();
        OnNodePing( nodeIP, response );
    }
    else
    {
        StartReceive();
        PLOG_ERR( "PingReceiverBoost::HandleRead error=" << error );
    }
}

} // namespace master


#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include "node_ping.h"
#include "common/log.h"
#include "common/protocol.h"
#include "worker_manager.h"

namespace master {

void PingReceiver::OnNodePing( const std::string &nodeIP, const std::string &msg )
{
    //PS_LOG( nodeIP << " : " << msg );

    std::string protocol, header, body;
    int version;
    if ( !common::Protocol::ParseMsg( msg, protocol, version, header, body ) )
    {
        PS_LOG( "PingReceiver::OnNodePing: couldn't parse msg: " << msg );
        return;
    }

    common::ProtocolCreator protocolCreator;
    boost::scoped_ptr< common::Protocol > parser(
        protocolCreator.Create( protocol, version )
    );
    if ( !parser )
    {
        PS_LOG( "PingReceiver::OnNodePing: appropriate parser not found for protocol: "
                << protocol << " " << version );
        return;
    }

    std::string type;
    parser->ParseMsgType( header, type );
    if ( !parser->ParseMsgType( header, type ) )
    {
        PS_LOG( "PingReceiver::OnNodePing: couldn't parse msg type: " << header );
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
            PS_LOG( "PingReceiver::OnNodePing: couldn't parse msg body: " << body );
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
            PS_LOG( "PingReceiver::OnNodePing: couldn't parse msg body: " << body );
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
        PS_LOG( "PingReceiverBoost::HandleRead error=" << error );
    }
}

} // namespace master


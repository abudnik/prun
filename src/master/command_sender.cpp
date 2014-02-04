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
#include "command_sender.h"
#include "worker_manager.h"
#include "job_manager.h"
#include "common/log.h"
#include "common/protocol.h"
#include "defines.h"

namespace master {

void CommandSender::Run()
{
    CommandPtr command;
    std::string hostIP;

    WorkerManager &workerMgr = WorkerManager::Instance();
    workerMgr.Subscribe( this, WorkerManager::eCommand );

    bool getCommand = false;
    while( !stopped_ )
    {
        if ( !getCommand )
        {
            boost::unique_lock< boost::mutex > lock( awakeMut_ );
            if ( !newCommandAvailable_ )
                awakeCond_.wait( lock );
            newCommandAvailable_ = false;
        }

        getCommand = workerMgr.GetCommand( command, hostIP );
        if ( getCommand )
        {
            PLOG( "Get command '" << command->GetCommand() << "' : " << hostIP );
            SendCommand( command, hostIP );
        }
    }
}

void CommandSender::Stop()
{
    stopped_ = true;
    boost::unique_lock< boost::mutex > lock( awakeMut_ );
    awakeCond_.notify_all();
}

void CommandSender::NotifyObserver( int event )
{
    boost::unique_lock< boost::mutex > lock( awakeMut_ );
    newCommandAvailable_ = true;
    awakeCond_.notify_all();
}

void CommandSender::OnSendCommand( bool success, int errCode, CommandPtr &command, const std::string &hostIP )
{
    if ( !success ) // retrieving of job result from message failed
    {
        errCode = -1;
        timeoutManager_->PushCommand( command, hostIP, command->GetRepeatDelay() );
    }
    command->OnExec( errCode, hostIP );
}

void CommandSenderBoost::Start()
{
    io_service_.post( boost::bind( &CommandSender::Run, this ) );
}

void CommandSenderBoost::Stop()
{
    CommandSender::Stop();

    cmdSenderSem_.Reset();
}

void CommandSenderBoost::SendCommand( CommandPtr &command, const std::string &hostIP )
{   
    cmdSenderSem_.Wait();

    RpcBoost::sender_ptr sender(
        new RpcBoost( io_service_, this, command, hostIP )
    );
    sender->SendCommand();
}

void CommandSenderBoost::OnSendCommand( bool success, int errCode, CommandPtr &command, const std::string &hostIP )
{
    cmdSenderSem_.Notify();
    CommandSender::OnSendCommand( success, errCode, command, hostIP );
}

void RpcBoost::SendCommand()
{
    tcp::endpoint nodeEndpoint(
        boost::asio::ip::address::from_string( hostIP_ ),
        NODE_PORT
    );

    socket_.async_connect( nodeEndpoint,
                           boost::bind( &RpcBoost::HandleConnect, shared_from_this(),
                                        boost::asio::placeholders::error ) );
}

void RpcBoost::HandleConnect( const boost::system::error_code &error )
{
    if ( !error )
    {
        MakeRequest();

        boost::asio::async_read( socket_,
                                 boost::asio::buffer( &buffer_, sizeof( char ) ),
                                 boost::bind( &RpcBoost::FirstRead, shared_from_this(),
                                              boost::asio::placeholders::error,
                                              boost::asio::placeholders::bytes_transferred ) );

        boost::asio::async_write( socket_,
                                  boost::asio::buffer( request_ ),
                                  boost::bind( &RpcBoost::HandleWrite, shared_from_this(),
                                               boost::asio::placeholders::error,
                                               boost::asio::placeholders::bytes_transferred ) );
    }
    else
    {
        PLOG_WRN( "RpcBoost::HandleConnect error=" << error.message() );
        OnCompletion( false, 0 );
    }
}

void RpcBoost::HandleWrite( const boost::system::error_code &error, size_t bytes_transferred )
{
    if ( error )
    {
        PLOG_WRN( "RpcBoost::HandleWrite error=" << error.message() );
        OnCompletion( false, 0 );
    }
}

void RpcBoost::FirstRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( !error )
    {
        if ( firstRead_ )
        {
            // skip node's read completion status byte
            firstRead_ = false;
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &RpcBoost::FirstRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
            return;
        }

        int ret = response_.OnFirstRead( buffer_, bytes_transferred );
        if ( ret == 0 )
        {
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &RpcBoost::FirstRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
            return;
        }
    }
    else
    {
        PLOG_WRN( "RpcBoost::FirstRead error=" << error.message() );
    }

    HandleRead( error, bytes_transferred );
}

void RpcBoost::HandleRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( !error )
    {
        response_.OnRead( buffer_, bytes_transferred );

        if ( !response_.IsReadCompleted() )
        {
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &RpcBoost::HandleRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
        }
        else
        {
            if ( !HandleResponse() )
            {
                OnCompletion( false, 0 );
            }
        }
    }
    else
    {
        PLOG_WRN( "RpcBoost::HandleRead error=" << error.message() );
        OnCompletion( false, 0 );
    }
}

bool RpcBoost::HandleResponse()
{
    const std::string &msg = response_.GetString();

    std::string protocol, header, body;
    int version;
    if ( !common::Protocol::ParseMsg( msg, protocol, version, header, body ) )
    {
        PLOG_ERR( "RpcBoost::HandleResponse: couldn't parse msg: " << msg );
        return false;
    }

    common::ProtocolCreator protocolCreator;
    boost::scoped_ptr< common::Protocol > parser(
        protocolCreator.Create( protocol, version )
    );
    if ( !parser )
    {
        PLOG_ERR( "RpcBoost::HandleResponse: appropriate parser not found for protocol: "
                  << protocol << " " << version );
        return false;
    }

    std::string type;
    parser->ParseMsgType( header, type );
    if ( !parser->ParseMsgType( header, type ) )
    {
        PLOG_ERR( "RpcBoost::HandleResponse: couldn't parse msg type: " << header );
        return false;
    }

    if ( type == "send_command_result" )
    {
        common::Demarshaller demarshaller;
        if ( parser->ParseBody( body, demarshaller.GetProperties() ) )
        {
            try
            {
                int errCode;
                demarshaller( "err_code", errCode );
                OnCompletion( true, errCode );
                return true;
            }
            catch( std::exception &e )
            {
                PLOG_ERR( "RpcBoost::HandleResponse: " << e.what() );
            }
        }
        else
        {
            PLOG_ERR( "RpcBoost::HandleResponse: couldn't parse msg body: " << body );
        }
    }
    else
    {
        PLOG_ERR( "RpcBoost::HandleResponse: unexpected msg type: " << type );
    }

    return false;
}

void RpcBoost::OnCompletion( bool success, int errCode )
{
    {
        boost::mutex::scoped_lock scoped_lock( completionMut_ );
        if ( completed_ )
            return;
        else
            completed_ = true;
    }

    sender_->OnSendCommand( success, errCode, command_, hostIP_ );
}


void RpcBoost::MakeRequest()
{
    common::ProtocolJson protocol;

    const std::string &masterId = JobManager::Instance().GetMasterId();

    protocol.SendCommand( request_, masterId, command_->GetCommand(), command_->GetAllParams() );
}

} // namespace master

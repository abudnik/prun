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

#ifndef __COMMAND_SENDER_H
#define __COMMAND_SENDER_H

#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include "common/observer.h"
#include "common/helper.h"
#include "common/request.h"
#include "command.h"
#include "timeout_manager.h"

using boost::asio::ip::tcp;

namespace master {

class CommandSender : common::Observer
{
public:
    CommandSender( TimeoutManager *timeoutManager )
    : stopped_( false ), timeoutManager_( timeoutManager ),
     newCommandAvailable_( false )
    {}

    virtual void Start() = 0;

    virtual void Stop();

    void Run();

    virtual void OnSendCommand( bool success, int errCode, CommandPtr &command, const std::string &hostIP );

private:
    virtual void NotifyObserver( int event );

    virtual void SendCommand( CommandPtr &command, const std::string &hostIP ) = 0;

private:
    bool stopped_;
    TimeoutManager *timeoutManager_;
    boost::mutex awakeMut_;
    boost::condition_variable awakeCond_;
    bool newCommandAvailable_;
};

class RpcBoost : public boost::enable_shared_from_this< RpcBoost >
{
    typedef boost::array< char, 32 * 1024 > BufferType;

public:
    typedef boost::shared_ptr< RpcBoost > sender_ptr;

public:
    RpcBoost( boost::asio::io_service &io_service,
                 CommandSender *sender, CommandPtr &command,
                 const std::string &hostIP )
    : socket_( io_service ),
     response_( false ), firstRead_( true ),
     sender_( sender ), command_( command ),
     hostIP_( hostIP )
    {}

    void SendCommand();

private:
    void HandleConnect( const boost::system::error_code &error );

    void MakeRequest();

    void HandleWrite( const boost::system::error_code &error, size_t bytes_transferred );

    void FirstRead( const boost::system::error_code &error, size_t bytes_transferred );

    void HandleRead( const boost::system::error_code &error, size_t bytes_transferred );

    bool HandleResponse();

private:
    tcp::socket socket_;
    BufferType buffer_;
    common::Request< BufferType > response_;
    bool firstRead_;
    std::string request_;
    CommandSender *sender_;
    CommandPtr command_;
    std::string hostIP_;
};

class CommandSenderBoost : public CommandSender
{
public:
    CommandSenderBoost( boost::asio::io_service &io_service,
                        TimeoutManager *timeoutManager,
                        int maxSimultCommandSenders )
    : CommandSender( timeoutManager ),
     io_service_( io_service ),
     cmdSenderSem_( maxSimultCommandSenders ),
     completed_( false )
    {}

    virtual void Start();

    virtual void Stop();

private:
    virtual void SendCommand( CommandPtr &command, const std::string &hostIP );

    virtual void OnSendCommand( bool success, int errCode, CommandPtr &command, const std::string &hostIP );

private:
    boost::asio::io_service &io_service_;
    common::Semaphore cmdSenderSem_;
    bool completed_;
    boost::mutex completionMut_;
};

} // namespace master

#endif

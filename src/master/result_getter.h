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

#ifndef __RESULT_GETTER_H
#define __RESULT_GETTER_H

#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include "common/observer.h"
#include "common/helper.h"
#include "common/request.h"
#include "worker.h"

using boost::asio::ip::tcp;

namespace master {

class ResultGetter : common::Observer
{
public:
    ResultGetter()
    : stopped_( false ), newJobAvailable_( false )
    {}

    virtual void Start() = 0;

    virtual void Stop();

    void Run();

    virtual void OnGetTaskResult( bool success, int errCode, int64_t execTime, const WorkerTask &workerTask, const std::string &hostIP );

private:
    virtual void NotifyObserver( int event );

    virtual void GetTaskResult( const WorkerTask &workerTask, const std::string &hostIP ) = 0;

private:
    bool stopped_;

    boost::mutex awakeMut_;
    boost::condition_variable awakeCond_;
    bool newJobAvailable_;
};

class GetterBoost : public boost::enable_shared_from_this< GetterBoost >
{
    typedef boost::array< char, 32 * 1024 > BufferType;

public:
    typedef boost::shared_ptr< GetterBoost > getter_ptr;

public:
    GetterBoost( boost::asio::io_service &io_service,
                 ResultGetter *getter, const WorkerTask &workerTask,
                 const std::string &hostIP )
    : socket_( io_service ),
     response_( false ), firstRead_( true ),
     getter_( getter ), workerTask_( workerTask ),
     hostIP_( hostIP ), completed_( false )
    {}

    void GetTaskResult();

private:
    void HandleConnect( const boost::system::error_code &error );

    void MakeRequest();

    void HandleWrite( const boost::system::error_code &error, size_t bytes_transferred );

    void FirstRead( const boost::system::error_code &error, size_t bytes_transferred );

    void HandleRead( const boost::system::error_code &error, size_t bytes_transferred );

    bool HandleResponse();

    void OnCompletion( bool success, int errCode, int64_t execTime );

private:
    tcp::socket socket_;
    BufferType buffer_;
    common::Request< BufferType > response_;
    bool firstRead_;
    std::string request_;
    ResultGetter *getter_;
    WorkerTask workerTask_;
    std::string hostIP_;
    bool completed_;
    boost::mutex completionMut_;
};

class ResultGetterBoost : public ResultGetter
{
public:
    ResultGetterBoost( boost::asio::io_service &io_service,
                       int maxSimultResultGetters )
    : io_service_( io_service ),
     getJobsSem_( maxSimultResultGetters )
    {}

    virtual void Start();

    virtual void Stop();

private:
    virtual void GetTaskResult( const WorkerTask &workerTask, const std::string &hostIP );

    virtual void OnGetTaskResult( bool success, int errCode, int64_t execTime, const WorkerTask &workerTask, const std::string &hostIP );

private:
    boost::asio::io_service &io_service_;
    common::Semaphore getJobsSem_;
};

} // namespace master

#endif

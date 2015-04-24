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

#ifndef __TIMEOUT_MANAGER_H
#define __TIMEOUT_MANAGER_H

#include <boost/asio.hpp>
#include <mutex>
#include "common/helper.h"
#include "worker.h"
#include "command.h"

namespace master {

struct ITimeoutManager
{
    virtual void PushJobQueue( int64_t jobId, int queueTimeout ) = 0;
    virtual void PushJob( int64_t jobId, int jobTimeout ) = 0;
    virtual void PushTask( const WorkerTask &task, const std::string &hostIP, int timeout ) = 0;

    virtual void PushCommand( CommandPtr &command, const std::string &hostIP, int delay ) = 0;
};


// hint: don't use boost::asio::deadline_timer due to os timer limitations (~16k or so)

class TimeoutManager : public ITimeoutManager
{
    typedef std::function< void () > Callback;
    typedef std::chrono::system_clock::time_point ptime;
    typedef std::multimap< ptime, Callback > TimeToCallback;

    struct TaskTimeoutHandler
    {
        void HandleTimeout();
        WorkerTask workerTask_;
        std::string hostIP_;
    };

    struct JobTimeoutHandler
    {
        void HandleTimeout();
        int64_t jobId_;
    };
    struct JobQueueTimeoutHandler
    {
        void HandleTimeout();
        int64_t jobId_;
    };
    struct StopTaskTimeoutHandler
    {
        void HandleTimeout();
        CommandPtr command_;
        std::string hostIP_;
    };

public:
    TimeoutManager( boost::asio::io_service &io_service )
    : io_service_( io_service ), stopped_( false )
    {}

    void Start();

    void Stop();

    void Run();

    virtual void PushJobQueue( int64_t jobId, int queueTimeout );
    virtual void PushJob( int64_t jobId, int jobTimeout );
    virtual void PushTask( const WorkerTask &task, const std::string &hostIP, int timeout );

    virtual void PushCommand( CommandPtr &command, const std::string &hostIP, int delay );

private:
    void CheckTimeouts();

private:
    boost::asio::io_service &io_service_;
    bool stopped_;
    common::SyncTimer timer_;
    TimeToCallback jobs_;
    std::mutex jobsMut_;
};

} // namespace master

#endif

/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2015 Andrey Budnik <budnik27@gmail.com>

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

#ifndef __CRON_MANAGER_H
#define __CRON_MANAGER_H

#include <boost/asio.hpp>
#include <mutex>
#include "common/helper.h"
#include "job.h"

namespace master {

struct ICronManager
{
    virtual void PushJob( const JobPtr &job, bool afterExecution ) = 0;
    virtual void PushMetaJob( const JobGroupPtr &metaJob, bool afterExecution ) = 0;
};

// TODO: deadlocks
class CronManager : public ICronManager
{
    typedef std::function< void () > Callback;
    typedef std::chrono::system_clock::time_point ptime;
    typedef std::multimap< ptime, Callback > TimeToCallback;

    struct JobTimeoutHandler
    {
        void HandleTimeout();
        std::string jobDescription_;
    };
    struct MetaJobTimeoutHandler
    {
        void HandleTimeout();
        std::string metaJobDescription_;
    };

public:
    CronManager( boost::asio::io_service &io_service )
    : io_service_( io_service ), stopped_( false )
    {}

    void Start();

    void Stop();

    void Run();

    // ICronManager
    virtual void PushJob( const JobPtr &job, bool afterExecution );
    virtual void PushMetaJob( const JobGroupPtr &metaJob, bool afterExecution );

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

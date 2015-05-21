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

class ICronVisitor;

struct ICronManager
{
    virtual void PushJob( const JobPtr &job, bool afterExecution ) = 0;
    virtual void PushMetaJob( const JobGroupPtr &metaJob ) = 0;
    virtual void PushMetaJob( std::list< JobPtr > &jobs ) = 0;
    virtual void StopJob( const std::string &jobName ) = 0;

    virtual void Accept( ICronVisitor *visitor ) = 0;
};

struct CronJobInfo
{
    std::string jobName_;
    std::time_t deadline_;
};

// TODO: deadlocks
class CronManager : public ICronManager
{
    struct TimeoutHandler;

    typedef std::chrono::system_clock::time_point ptime;
    typedef std::shared_ptr< TimeoutHandler > CallbackPtr;
    typedef std::multimap< ptime, CallbackPtr > TimeToCallback;
    typedef std::map< std::string, CallbackPtr > JobNameToCallback;

    struct TimeoutHandler
    {
        TimeoutHandler();
        virtual void HandleTimeout() = 0;

        ptime deadline_;
        std::string jobDescription_;
        std::string jobName_;
        bool removed_;
    };
    struct JobTimeoutHandler : TimeoutHandler
    {
        virtual void HandleTimeout();
    };
    struct MetaJobTimeoutHandler : TimeoutHandler
    {
        virtual void HandleTimeout();

        std::set< std::string > jobNames;
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
    virtual void PushMetaJob( const JobGroupPtr &metaJob );
    virtual void PushMetaJob( std::list< JobPtr > &jobs );
    virtual void StopJob( const std::string &jobName );

    virtual void Accept( ICronVisitor *visitor );

    void GetJobsInfo( std::vector< CronJobInfo > &names );

private:
    void CheckTimeouts();

private:
    boost::asio::io_service &io_service_;
    bool stopped_;
    common::SyncTimer timer_;
    TimeToCallback jobs_;
    JobNameToCallback names_;
    std::mutex jobsMut_;
};

} // namespace master

#endif

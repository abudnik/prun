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

#ifndef __JOB_SENDER_H
#define __JOB_SENDER_H

#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include "common/observer.h"
#include "common/helper.h"
#include "job.h"
#include "worker.h"
#include "timeout_manager.h"

using boost::asio::ip::tcp;

namespace master {

class JobSender : common::Observer
{
public:
    JobSender( TimeoutManager *timeoutManager )
    : stopped_( false ), timeoutManager_( timeoutManager ),
     newJobAvailable_( false )
    {}

    virtual void Start() = 0;

    virtual void Stop();

    void Run();

    virtual void OnJobSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const JobPtr &job );

private:
    virtual void NotifyObserver( int event );

    virtual void SendJob( const WorkerJob &workerJob, const std::string &hostIP, JobPtr &job ) = 0;

private:
    bool stopped_;
    TimeoutManager *timeoutManager_;
    boost::mutex awakeMut_;
    boost::condition_variable awakeCond_;
    bool newJobAvailable_;
};

class SenderBoost : public boost::enable_shared_from_this< SenderBoost >
{
public:
    typedef boost::shared_ptr< SenderBoost > sender_ptr;

public:
    SenderBoost( boost::asio::io_service &io_service,
                 JobSender *sender, const WorkerJob &workerJob,
                 const std::string &hostIP, JobPtr &job )
    : socket_( io_service ),
     sender_( sender ), workerJob_( workerJob ),
     hostIP_( hostIP ), job_( job )
    {}

    void Send();

private:
    void HandleConnect( const boost::system::error_code &error );

    void MakeRequest();

    void HandleWrite( const boost::system::error_code &error, size_t bytes_transferred );

    void HandleRead( const boost::system::error_code &error, size_t bytes_transferred );

private:
    tcp::socket socket_;
    std::string request_;
    char response_;
    JobSender *sender_;
    WorkerJob workerJob_;
    std::string hostIP_;
    JobPtr job_;
};

class JobSenderBoost : public JobSender
{
public:
    JobSenderBoost( boost::asio::io_service &io_service,
                    TimeoutManager *timeoutManager,
                    int maxSimultSendingJobs )
    : JobSender( timeoutManager ),
     io_service_( io_service ),
     sendJobsSem_( maxSimultSendingJobs )
    {}

    virtual void Start();

    virtual void Stop();

private:
    virtual void SendJob( const WorkerJob &workerJob, const std::string &hostIP, JobPtr &job );

    virtual void OnJobSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP, const JobPtr &job );

private:
    boost::asio::io_service &io_service_;
    common::Semaphore sendJobsSem_;
};

} // namespace master

#endif

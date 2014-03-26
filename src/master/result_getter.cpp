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
#include "result_getter.h"
#include "worker_manager.h"
#include "job_manager.h"
#include "scheduler.h"
#include "common/log.h"
#include "common/protocol.h"
#include "common/service_locator.h"
#include "defines.h"

namespace master {

void ResultGetter::Run()
{
    WorkerTask workerTask;
    std::string hostIP;

    WorkerManager &workerMgr = WorkerManager::Instance();
    workerMgr.Subscribe( this, WorkerManager::eTaskCompletion );

    bool getTask = false;
    while( !stopped_ )
    {
        if ( !getTask )
        {
            boost::unique_lock< boost::mutex > lock( awakeMut_ );
            if ( !newJobAvailable_ )
                awakeCond_.wait( lock );
            newJobAvailable_ = false;
        }

        getTask = workerMgr.GetAchievedTask( workerTask, hostIP );
        if ( getTask )
        {
            PLOG( "Get achieved work " << workerTask.GetJobId() << " : " << workerTask.GetTaskId() );
            GetTaskResult( workerTask, hostIP );
        }
    }
}

void ResultGetter::Stop()
{
    stopped_ = true;
    boost::unique_lock< boost::mutex > lock( awakeMut_ );
    awakeCond_.notify_all();
}

void ResultGetter::NotifyObserver( int event )
{
    boost::unique_lock< boost::mutex > lock( awakeMut_ );
    newJobAvailable_ = true;
    awakeCond_.notify_all();
}

void ResultGetter::OnGetTaskResult( bool success, int errCode, int64_t execTime, const WorkerTask &workerTask, const std::string &hostIP )
{
    if ( !success ) // retrieving of job result from message failed
        errCode = -1;
    Scheduler::Instance().OnTaskCompletion( errCode, execTime, workerTask, hostIP );
}

void ResultGetterBoost::Start()
{
    io_service_.post( boost::bind( &ResultGetter::Run, this ) );
}

void ResultGetterBoost::Stop()
{
    ResultGetter::Stop();
    
    getJobsSem_.Reset();
}

void ResultGetterBoost::GetTaskResult( const WorkerTask &workerTask, const std::string &hostIP )
{   
    getJobsSem_.Wait();

    GetterBoost::getter_ptr getter(
        new GetterBoost( io_service_, this, workerTask, hostIP )
    );
    getter->GetTaskResult();
}

void ResultGetterBoost::OnGetTaskResult( bool success, int errCode, int64_t execTime, const WorkerTask &workerTask, const std::string &hostIP )
{
    getJobsSem_.Notify();
    ResultGetter::OnGetTaskResult( success, errCode, execTime, workerTask, hostIP );
}

void GetterBoost::GetTaskResult()
{
    tcp::endpoint nodeEndpoint(
        boost::asio::ip::address::from_string( hostIP_ ),
        NODE_PORT
    );

    socket_.async_connect( nodeEndpoint,
                           boost::bind( &GetterBoost::HandleConnect, shared_from_this(),
                                        boost::asio::placeholders::error ) );
}

void GetterBoost::HandleConnect( const boost::system::error_code &error )
{
    if ( !error )
    {
        MakeRequest();

        boost::asio::async_read( socket_,
                                 boost::asio::buffer( &buffer_, sizeof( char ) ),
                                 boost::bind( &GetterBoost::FirstRead, shared_from_this(),
                                              boost::asio::placeholders::error,
                                              boost::asio::placeholders::bytes_transferred ) );

        boost::asio::async_write( socket_,
                                  boost::asio::buffer( request_ ),
                                  boost::bind( &GetterBoost::HandleWrite, shared_from_this(),
                                               boost::asio::placeholders::error,
                                               boost::asio::placeholders::bytes_transferred ) );
    }
    else
    {
        PLOG_WRN( "GetterBoost::HandleConnect error=" << error.message() );
        OnCompletion( false, 0, 0 );
    }
}

void GetterBoost::HandleWrite( const boost::system::error_code &error, size_t bytes_transferred )
{
    if ( error )
    {
        PLOG_WRN( "GetterBoost::HandleWrite error=" << error.message() );
        OnCompletion( false, 0, 0 );
    }
}

void GetterBoost::FirstRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( !error )
    {
        if ( firstRead_ )
        {
            // skip node's read completion status byte
            firstRead_ = false;
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &GetterBoost::FirstRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
            return;
        }

        int ret = response_.OnFirstRead( buffer_, bytes_transferred );
        if ( ret == 0 )
        {
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &GetterBoost::FirstRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
            return;
        }
    }
    else
    {
        PLOG_WRN( "GetterBoost::FirstRead error=" << error.message() );
    }

    HandleRead( error, bytes_transferred );
}

void GetterBoost::HandleRead( const boost::system::error_code& error, size_t bytes_transferred )
{
    if ( !error )
    {
        response_.OnRead( buffer_, bytes_transferred );

        if ( !response_.IsReadCompleted() )
        {
            socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &GetterBoost::HandleRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );
        }
        else
        {
            if ( !HandleResponse() )
            {
                OnCompletion( false, 0, 0 );
            }
        }
    }
    else
    {
        PLOG_WRN( "GetterBoost::HandleRead error=" << error.message() );
        getter_->OnGetTaskResult( false, 0, 0, workerTask_, hostIP_ );
    }
}

bool GetterBoost::HandleResponse()
{
    const std::string &msg = response_.GetString();

    std::string protocol, header, body;
    int version;
    if ( !common::Protocol::ParseMsg( msg, protocol, version, header, body ) )
    {
        PLOG_ERR( "GetterBoost::HandleResponse: couldn't parse msg: " << msg );
        return false;
    }

    common::ProtocolCreator protocolCreator;
    boost::scoped_ptr< common::Protocol > parser(
        protocolCreator.Create( protocol, version )
    );
    if ( !parser )
    {
        PLOG_ERR( "GetterBoost::HandleResponse: appropriate parser not found for protocol: "
                  << protocol << " " << version );
        return false;
    }

    std::string type;
    parser->ParseMsgType( header, type );
    if ( !parser->ParseMsgType( header, type ) )
    {
        PLOG_ERR( "GetterBoost::HandleResponse: couldn't parse msg type: " << header );
        return false;
    }

    if ( type == "send_job_result" )
    {
        common::Demarshaller demarshaller;
        if ( parser->ParseBody( body, demarshaller.GetProperties() ) )
        {
            try
            {
                int errCode;
                int64_t execTime;
                demarshaller( "err_code", errCode )( "elapsed", execTime );
                OnCompletion( true, errCode, execTime );
                return true;
            }
            catch( std::exception &e )
            {
                PLOG_ERR( "GetterBoost::HandleResponse: " << e.what() );
            }
        }
        else
        {
            PLOG_ERR( "GetterBoost::HandleResponse: couldn't parse msg body: " << body );
        }
    }
    else
    {
        PLOG_ERR( "GetterBoost::HandleResponse: unexpected msg type: " << type );
    }

    return false;
}

void GetterBoost::OnCompletion( bool success, int errCode, int64_t execTime )
{
    {
        boost::mutex::scoped_lock scoped_lock( completionMut_ );
        if ( completed_ )
            return;
        else
            completed_ = true;
    }

    getter_->OnGetTaskResult( success, errCode, execTime, workerTask_, hostIP_ );
}

void GetterBoost::MakeRequest()
{
    common::ProtocolJson protocol;

    IJobManager *jobManager = common::ServiceLocator::Instance().Get< IJobManager >();
    const std::string &masterId = jobManager->GetMasterId();

    common::Marshaller marshaller;
    marshaller( "master_id", masterId )
        ( "job_id", workerTask_.GetJobId() )
        ( "task_id", workerTask_.GetTaskId() );

    protocol.Serialize( request_, "get_result", marshaller );
}

} // namespace master

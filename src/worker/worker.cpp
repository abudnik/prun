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

#define BOOST_SPIRIT_THREADSAFE

#include <iostream>
#include <boost/program_options.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/thread.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/filesystem/operations.hpp>
#include <unistd.h>
#include <csignal>
#include <sys/wait.h>
#include <cstdlib>
#include "common/helper.h"
#include "common/log.h"
#include "common/config.h"
#include "common/pidfile.h"
#include "common/daemon.h"
#include "common/request.h"
#include "common.h"
#include "comm_descriptor.h"
#include "master_ping.h"
#include "node_job.h"
#include "exec_info.h"
#include "job_completion_ping.h"
#include "computer_info.h"


using namespace std;
using boost::asio::ip::tcp;

namespace worker {

pid_t prexecPid;

common::Semaphore *requestSem;

CommDescrPool *commDescrPool;

ExecTable execTable, pendingTable;


class Action
{
public:
    virtual void Execute( const boost::shared_ptr< Job > &job ) = 0;
    virtual ~Action() {}
};

class NoAction : public Action
{
    virtual void Execute( const boost::shared_ptr< Job > &job ) {}
};

class PrExecConnection : public boost::enable_shared_from_this< PrExecConnection >
{
    typedef boost::array< char, 2048 > BufferType;

public:
    typedef boost::shared_ptr< PrExecConnection > connection_ptr;

public:
    PrExecConnection()
    : socket_( NULL ),
     response_( true )
    {}

    bool Init()
    {
        if ( commDescrPool->AllocCommDescr() )
        {
            CommDescr &commDescr = commDescrPool->GetCommDescr();
            socket_ = commDescr.socket.get();
            memset( buffer_.c_array(), 0, buffer_.size() );
            return true;
        }
        else
        {
            PLOG_WRN( "PrExecConnection::Init: AllocCommDescr failed" );
        }
        return false;
    }

    void Release()
    {
        if ( socket_ )
        {
            commDescrPool->FreeCommDescr();
        }
    }

    int Send( const std::string &message )
    {
        if ( !socket_ )
        {
            PLOG_WRN( "PrExecConnection::Send: socket is not available" );
            return NODE_FATAL;
        }

        errCode_ = 0;

        try
        {
            boost::asio::async_write( *socket_,
                                      boost::asio::buffer( message ),
                                      boost::bind( &PrExecConnection::HandleWrite, shared_from_this(),
                                                   boost::asio::placeholders::error,
                                                   boost::asio::placeholders::bytes_transferred ) );

            boost::system::error_code error;
            bool firstRead = true;
            while( true )
            {
                size_t bytes_transferred = socket_->read_some( boost::asio::buffer( buffer_ ), error );
                if ( !bytes_transferred )
                {
                    PLOG_ERR( "PrExecConnection::Send: read_some failed, error=" << error.message() );
                    errCode_ = NODE_FATAL;
                    break;
                }

                if ( firstRead )
                {
                    int ret = response_.OnFirstRead( buffer_, bytes_transferred );
                    firstRead = ( ret == 0 );
                }
                if ( !firstRead )
                {
                    response_.OnRead( buffer_, bytes_transferred );

                    if ( response_.IsReadCompleted() )
                    {
                        HandleResponse();
                        break;
                    }
                }
            }
        }
        catch( boost::system::system_error &e )
        {
            PLOG_ERR( "PrExecConnection::Send() failed: " << e.what() );
            errCode_ = NODE_FATAL;
        }

        return errCode_;
    }

    const boost::property_tree::ptree &GetResponsePtree() const { return responsePtree_; }

private:
    void HandleWrite( const boost::system::error_code& error, size_t bytes_transferred )
    {
        if ( error )
        {
            PLOG_ERR( "PrExecConnection::HandleWrite error=" << error.message() );
        }
    }

    void HandleResponse()
    {
        std::istringstream ss( response_.GetString() );

        boost::property_tree::read_json( ss, responsePtree_ );
        errCode_ = responsePtree_.get<int>( "err" );
    }

private:
    tcp::socket *socket_;
    BufferType buffer_;
    boost::property_tree::ptree responsePtree_;
    common::Request< BufferType > response_;
    int errCode_;
};

class ExecuteTask : public Action
{
    virtual void Execute( const boost::shared_ptr< Job > &job )
    {
        const Job::Tasks &tasks = job->GetTasks();
        if ( tasks.empty() )
        {
            PLOG_WRN( "ExecuteTask::Execute: empty tasks for jobId=" << job->GetJobId() );
            job->OnError( NODE_FATAL );
            SaveCompletionResults( job );
            return;
        }

        if ( !ExpandFilePath( job ) )
        {
            job->OnError( NODE_SCRIPT_FILE_NOT_FOUND );
            SaveCompletionResults( job );
            return;
        }

        boost::asio::io_service *io_service = commDescrPool->GetIoService();
        Job::Tasks::const_iterator it = tasks.begin();
        for( ; it != tasks.end(); ++it )
        {
            ExecInfo execInfo;
            execInfo.jobId_ = job->GetJobId();
            execInfo.taskId_ = *it;
            execInfo.masterId_ = job->GetMasterId();
            pendingTable.Add( execInfo );

            io_service->post( boost::bind( &ExecuteTask::DoSend,
                                           boost::shared_ptr< ExecuteTask >( new ExecuteTask ),
                                           boost::shared_ptr< Job >( job ), *it ) );
        }
    }

    void SaveCompletionResults( const boost::shared_ptr< Job > &job, int taskId, int64_t execTime ) const
    {
        JobDescriptor descr;
        JobCompletionStat stat;
        descr.jobId = job->GetJobId();
        descr.taskId = taskId;
        descr.masterIP = job->GetMasterIP();
        descr.masterId = job->GetMasterId();
        stat.errCode = job->GetErrorCode();
        stat.execTime = execTime;
        JobCompletionTable::Instance().Set( descr, stat );
    }

    void SaveCompletionResults( const boost::shared_ptr< Job > &job ) const
    {
        JobDescriptor descr;
        JobCompletionStat stat;
        descr.jobId = job->GetJobId();
        descr.masterIP = job->GetMasterIP();
        descr.masterId = job->GetMasterId();
        stat.errCode = job->GetErrorCode();

        const Job::Tasks &tasks = job->GetTasks();
        Job::Tasks::const_iterator it = tasks.begin();
        for( ; it != tasks.end(); ++it )
        {
            descr.taskId = *it;
            JobCompletionTable::Instance().Set( descr, stat );
        }
    }

    void NodeJobCompletionPing( const boost::shared_ptr< Job > &job, int taskId )
    {
        boost::asio::io_service *io_service = commDescrPool->GetIoService();

        using boost::asio::ip::udp;
        boost::asio::ip::address address( boost::asio::ip::address::from_string( job->GetMasterIP() ) );
        udp::endpoint master_endpoint( address, DEFAULT_MASTER_UDP_PORT );
        udp::socket socket( *io_service, udp::endpoint( master_endpoint.protocol(), 0 ) );

        common::ProtocolJson protocol;
        std::string msg;
        protocol.NodeJobCompletionPing( msg, job->GetJobId(), taskId );

        try
        {
            socket.send_to( boost::asio::buffer( msg ), master_endpoint );
        }
        catch( boost::system::system_error &e )
        {
            PLOG_ERR( "ExecuteTask::NodeJobCompletionPing: send_to failed: " << e.what() << ", host : " << master_endpoint );
        }
    }

    bool ExpandFilePath( const boost::shared_ptr< Job > &job )
    {
        if ( job->GetScriptLength() > 0 )
            return true;

        const std::string &filePath = job->GetFilePath();

        bool fileExists = boost::filesystem::exists( filePath );
        if ( fileExists )
        {
            job->SetFilePath( filePath );
            return true;
        }
        PLOG_WRN( "ExecuteTask::ExpandFilePath: file not exists '" << filePath << "'" );
        return false;
    }

public:
    void DoSend( const boost::shared_ptr< Job > &job, int taskId )
    {
        PrExecConnection::connection_ptr prExecConnection( new PrExecConnection() );
        if ( !prExecConnection->Init() )
            return;

        ExecInfo execInfo;
        execInfo.jobId_ = job->GetJobId();
        execInfo.taskId_ = taskId;
        execInfo.masterId_ = job->GetMasterId();
        execTable.Add( execInfo );

        // write script into shared memory
        CommDescr &commDescr = commDescrPool->GetCommDescr();
        const std::string &script = job->GetScript();
        if ( script.size() )
        {
            memcpy( commDescr.shmemAddr, script.c_str(), script.size() );
            char *shmemRequestEnd = commDescr.shmemAddr + script.size();
            *shmemRequestEnd = '\0';
        }

        // prepare json command
        boost::property_tree::ptree ptree;
        std::ostringstream ss, ss2;

        ptree.put( "task", "exec" );
        ptree.put( "id", commDescr.shmemBlockId );
        ptree.put( "len", script.size() );
        ptree.put( "lang", job->GetScriptLanguage() );
        ptree.put( "path", job->GetFilePath() );
        ptree.put( "job_id", job->GetJobId() );
        ptree.put( "task_id", taskId );
        ptree.put( "master_id", job->GetMasterId() );
        ptree.put( "num_tasks", job->GetNumTasks() );
        ptree.put( "timeout", job->GetTimeout() );

        boost::property_tree::write_json( ss, ptree, false );

        ss2 << ss.str().size() << '\n' << ss.str();

        int errCode;

        if ( pendingTable.Delete( job->GetJobId(), taskId, job->GetMasterId() ) )
        {
            errCode = prExecConnection->Send( ss2.str() );
        }
        else
        {
            errCode = NODE_JOB_TIMEOUT;
        }
        job->OnError( errCode );

        prExecConnection->Release();

        // save completion results and ping master
        execTable.Delete( job->GetJobId(), taskId, job->GetMasterId() );
        const boost::property_tree::ptree &responsePtree = prExecConnection->GetResponsePtree();
        int64_t execTime = 0;
        try
        {
            execTime = responsePtree.get< int64_t >( "elapsed" );
        }
        catch( std::exception &e )
        {
            PLOG( "ExecuteTask::DoSend: " << e.what() );
        }

        SaveCompletionResults( job, taskId, execTime );

        NodeJobCompletionPing( job, taskId );
    }
};

class StopTask : public Action
{
    virtual void Execute( const boost::shared_ptr< Job > &job )
    {
        if ( pendingTable.Delete( job->GetJobId(), job->GetTaskId(), job->GetMasterId() ) )
        {
            job->OnError( NODE_JOB_TIMEOUT );
        }

        if ( !execTable.Contains( job->GetJobId(), job->GetTaskId(), job->GetMasterId() ) )
        {
            job->OnError( NODE_TASK_NOT_FOUND );
            return;
        }

        PrExecConnection::connection_ptr prExecConnection( new PrExecConnection() );
        if ( !prExecConnection->Init() )
            return;

        // prepare json command
        boost::property_tree::ptree ptree;
        std::ostringstream ss, ss2;

        ptree.put( "task", "stop_task" );
        ptree.put( "job_id", job->GetJobId() );
        ptree.put( "task_id", job->GetTaskId() );
        ptree.put( "master_id", job->GetMasterId() );

        boost::property_tree::write_json( ss, ptree, false );

        ss2 << ss.str().size() << '\n' << ss.str();

        int errCode = prExecConnection->Send( ss2.str() );
        job->OnError( errCode );

        prExecConnection->Release();
    }
};

class StopPreviousJobs : public Action
{
    virtual void Execute( const boost::shared_ptr< Job > &job )
    {
        PrExecConnection::connection_ptr prExecConnection( new PrExecConnection() );
        if ( !prExecConnection->Init() )
            return;

        // prepare json command
        boost::property_tree::ptree ptree;
        std::ostringstream ss, ss2;

        ptree.put( "task", "stop_prev" );
        ptree.put( "master_id", job->GetMasterId() );

        boost::property_tree::write_json( ss, ptree, false );

        ss2 << ss.str().size() << '\n' << ss.str();

        int errCode = prExecConnection->Send( ss2.str() );
        job->OnError( errCode );

        prExecConnection->Release();
    }
};

class StopAllJobs : public Action
{
    virtual void Execute( const boost::shared_ptr< Job > &job )
    {
        pendingTable.Clear();

        PrExecConnection::connection_ptr prExecConnection( new PrExecConnection() );
        if ( !prExecConnection->Init() )
            return;

        // prepare json command
        boost::property_tree::ptree ptree;
        std::ostringstream ss, ss2;

        ptree.put( "task", "stop_all" );

        boost::property_tree::write_json( ss, ptree, false );

        ss2 << ss.str().size() << '\n' << ss.str();

        int errCode = prExecConnection->Send( ss2.str() );
        job->OnError( errCode );

        prExecConnection->Release();
    }
};

class ActionCreator
{
public:
    virtual Action *Create( const std::string &taskType )
    {
        if ( taskType == "exec" )
            return new ExecuteTask();
        if ( taskType == "get_result" )
            return new NoAction();
        if ( taskType == "stop_task" )
            return new StopTask();
        if ( taskType == "stop_prev" )
            return new StopPreviousJobs();
        if ( taskType == "stop_all" )
            return new StopAllJobs();
        return NULL;
    }
};

class Session
{
public:
    Session()
    : job_( new Job )
    {}

    virtual ~Session()
    {
        requestSem->Notify();
        cout << "S: ~Session()" << endl;
    }

protected:
    template< typename T >
    void HandleRequest( T &request )
    {
        if ( job_->ParseRequest( request ) )
        {
            boost::scoped_ptr< Action > action(
                actionCreator_.Create( job_->GetTaskType() )
            );
            if ( action )
            {
                action->Execute( job_ );
            }
            else
            {
                PLOG_WRN( "Session::HandleRequest: appropriate action not found for task type: "
                          << job_->GetTaskType() );
                job_->OnError( NODE_FATAL );
            }
        }
        else
        {
            PLOG_ERR( request.GetString() );
            job_->OnError( NODE_FATAL );
        }
    }

protected:
    boost::shared_ptr< Job > job_;

private:
    ActionCreator actionCreator_;
};

class BoostSession : public Session, public boost::enable_shared_from_this< BoostSession >
{
public:
    typedef boost::array< char, 32 * 1024 > BufferType;

public:
    BoostSession( boost::asio::io_service &io_service )
    : socket_( io_service ),
     request_( false )
    {}

    void Start()
    {
        boost::asio::ip::address remoteAddress = socket_.remote_endpoint().address();
        job_->SetMasterIP( remoteAddress.to_string() );

        socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                 boost::bind( &BoostSession::FirstRead, shared_from_this(),
                                              boost::asio::placeholders::error,
                                              boost::asio::placeholders::bytes_transferred ) );
    }

    tcp::socket &GetSocket()
    {
        return socket_;
    }

private:
    void FirstRead( const boost::system::error_code& error, size_t bytes_transferred )
    {
        if ( !error )
        {
            int ret = request_.OnFirstRead( buffer_, bytes_transferred );
            if ( ret == 0 )
            {
                socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                         boost::bind( &BoostSession::FirstRead, shared_from_this(),
                                                      boost::asio::placeholders::error,
                                                      boost::asio::placeholders::bytes_transferred ) );
                return;
            }
            if ( ret < 0 )
            {
                OnReadCompletion( false );
                return;
            }
        }
        else
        {
            PLOG_WRN( "SessionBoost::FirstRead error=" << error.message() );
        }

        HandleRead( error, bytes_transferred );
    }

    void HandleRead( const boost::system::error_code& error, size_t bytes_transferred )
    {
        if ( !error )
        {
            request_.OnRead( buffer_, bytes_transferred );

            if ( !request_.IsReadCompleted() )
            {
                socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                         boost::bind( &BoostSession::HandleRead, shared_from_this(),
                                                      boost::asio::placeholders::error,
                                                      boost::asio::placeholders::bytes_transferred ) );
            }
            else
            {
                OnReadCompletion( true );
                HandleRequest( request_ );
                WriteResponse();
            }
        }
        else
        {
            PLOG_WRN( "SessionBoost::HandleRead error=" << error.message() );
            OnReadCompletion( false );
        }
    }

    void OnReadCompletion( bool success )
    {
        readStatus_ = success ? '1' : '0';
        boost::asio::async_write( socket_,
                                  boost::asio::buffer( &readStatus_, sizeof( readStatus_ ) ),
                                  boost::bind( &BoostSession::HandleWrite, shared_from_this(),
                                               boost::asio::placeholders::error ) );
    }

    void WriteResponse()
    {
        job_->GetResponse( response_ );
        if ( !response_.empty() )
        {
            boost::asio::async_write( socket_,
                                      boost::asio::buffer( response_ ),
                                      boost::bind( &BoostSession::HandleWrite, shared_from_this(),
                                                   boost::asio::placeholders::error ) );
        }
    }

    void HandleWrite( const boost::system::error_code& error )
    {
        if ( error )
        {
            PLOG_ERR( "SessionBoost::HandleWrite error=" << error.message() );
        }
    }

protected:
    tcp::socket socket_;
    BufferType buffer_;
    common::Request< BufferType > request_;
    char readStatus_;
    std::string response_;
};


class ConnectionAcceptor
{
    typedef boost::shared_ptr< BoostSession > session_ptr;

public:
    ConnectionAcceptor( boost::asio::io_service &io_service, unsigned short port )
    : io_service_( io_service ),
      acceptor_( io_service )
    {
        try
        {
            common::Config &cfg = common::Config::Instance();
            bool ipv6 = cfg.Get<bool>( "ipv6" );

            tcp::endpoint endpoint( ipv6 ? tcp::v6() : tcp::v4(), port );
            acceptor_.open( endpoint.protocol() );
            acceptor_.set_option( tcp::acceptor::reuse_address( true ) );
            acceptor_.set_option( tcp::no_delay( true ) );
            acceptor_.bind( endpoint );
            acceptor_.listen();
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "ConnectionAcceptor: " << e.what() );
        }

        StartAccept();
    }

private:
    void StartAccept()
    {
        session_ptr session( new BoostSession( io_service_ ) );
        acceptor_.async_accept( session->GetSocket(),
                                boost::bind( &ConnectionAcceptor::HandleAccept, this,
                                             session, boost::asio::placeholders::error ) );
    }

    void HandleAccept( session_ptr session, const boost::system::error_code &error )
    {
        if ( !error )
        {
            cout << "connection accepted..." << endl;
            requestSem->Wait();
            io_service_.post( boost::bind( &BoostSession::Start, session ) );
            StartAccept();
        }
        else
        {
            PLOG_ERR( "HandleAccept: " << error.message() );
        }
    }

private:
    boost::asio::io_service &io_service_;
    tcp::acceptor acceptor_;
};

} // namespace worker


namespace {

void VerifyCommandlineParams( uid_t uid )
{
    if ( uid )
    {
        // check uid existance
        char line[256] = { '\0' };

        std::ostringstream command;
        command << "getent passwd " << uid << "|cut -d: -f1";

        FILE *cmd = popen( command.str().c_str(), "r" );
        fgets( line, sizeof(line), cmd );
        pclose( cmd );

        if ( !strlen( line ) )
        {
            std::cout << "Unknown uid: " << uid << std::endl;
            exit( 1 );
        }
    }
    else
    if ( getuid() == 0 )
    {
        std::cout << "Could not execute python code due to security issues" << std::endl <<
            "Please use --u command line parameter for using uid of non-privileged user" << std::endl;
        exit( 1 );
    }
}

void SigHandler( int s )
{
    if ( s == SIGCHLD )
    {
        int status;
        waitpid( -1, &status, WNOHANG );
    }
}

void SetupSignalHandlers()
{
    struct sigaction sigHandler;
    memset( &sigHandler, 0, sizeof( sigHandler ) );
    sigHandler.sa_handler = SigHandler;
    sigemptyset(&sigHandler.sa_mask);
    sigHandler.sa_flags = 0;

    sigaction( SIGCHLD, &sigHandler, NULL );
}

void SetupSignalMask()
{
    sigset_t sigset;
    sigemptyset( &sigset );
    sigaddset( &sigset, SIGTERM );
    sigaddset( &sigset, SIGHUP );
    sigaddset( &sigset, SIGCHLD );
    sigaddset( &sigset, SIGUSR1 );
    sigprocmask( SIG_BLOCK, &sigset, NULL );
}

void UnblockSighandlerMask()
{
    sigset_t sigset;
    // appropriate unblocking signals see in SetupSignalHandlers
    sigemptyset( &sigset );
    sigaddset( &sigset, SIGCHLD );
    pthread_sigmask( SIG_UNBLOCK, &sigset, NULL );
}

void UserInteraction()
{
    while( !getchar() );
}


void AtExit()
{
    namespace ipc = boost::interprocess;

    // send stop signal to PrExec proccess
    kill( worker::prexecPid, SIGTERM );

    // remove shared memory
    ipc::shared_memory_object::remove( worker::SHMEM_NAME );

    delete worker::requestSem;

    common::logger::ShutdownLogger();
}

void ThreadFun( boost::asio::io_service *io_service )
{
    try
    {
        io_service->run();
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ThreadFun: " << e.what() );
    }
}

class WorkerApplication
{
public:
    WorkerApplication( const std::string &exeDir, const std::string &cfgDir,
                       const std::string &resourcesDir, bool isDaemon, uid_t uid )
    : exeDir_( exeDir ),
     cfgDir_( cfgDir ),
     resourcesDir_( resourcesDir ),
     isDaemon_( isDaemon ), uid_( uid )
    {}

    void Initialize()
    {
        worker::ComputerInfo &compInfo = worker::ComputerInfo::Instance();
        unsigned int numExecThread = compInfo.GetNumCPU();
        numRequestThread_ = 2 * numExecThread;
        numThread_ = numRequestThread_ + 1; // +1 for accept thread

        common::logger::InitLogger( isDaemon_, "pworker" );

        common::Config &cfg = common::Config::Instance();
        if ( cfgDir_.empty() )
        {
            cfg.ParseConfig( exeDir_.c_str(), "worker.cfg" );
        }
        else
        {
            cfg.ParseConfig( "", cfgDir_.c_str() );
        }

        worker::JobCompletionTable::Instance();

        SetupSignalHandlers();
        SetupSignalMask();
        atexit( AtExit );

        SetupPrExecIPC();
        RunPrExecProcess();

        // create master request handlers
        work_.reset( new boost::asio::io_service::work( io_service_ ) );

        worker::requestSem = new common::Semaphore( numRequestThread_ );

        // create thread pool
        for( unsigned int i = 0; i < numThread_; ++i )
        {
            worker_threads_.create_thread(
                boost::bind( &ThreadFun, &io_service_ )
            );
        }

        // create pool for execute task actions
        work_exec_.reset( new boost::asio::io_service::work( io_service_exec_ ) );

        commDescrPool_.reset( new worker::CommDescrPool( numRequestThread_, &io_service_exec_,
                                                         (char*)mappedRegion_->get_address() ) );
        worker::commDescrPool = commDescrPool_.get();

        for( unsigned int i = 0; i < numExecThread; ++i )
        {
            worker_threads_.create_thread(
                boost::bind( &ThreadFun, &io_service_exec_ )
            );
        }

        // start acceptor
        acceptor_.reset( new worker::ConnectionAcceptor( io_service_, worker::DEFAULT_PORT ) );

        // create master ping handlers
        int completionPingTimeout = common::Config::Instance().Get<int>( "completion_ping_timeout" );
        completionPing_.reset( new worker::JobCompletionPingerBoost( io_service_ping_, completionPingTimeout ) );
        completionPing_->StartPing();

        masterPing_.reset( new worker::MasterPingBoost( io_service_ping_ ) );
        masterPing_->Start();

        // create thread pool for pingers
        // 1. job completion pinger
        // 2. master heartbeat ping receiver
        unsigned int numPingThread = 2;
        for( unsigned int i = 0; i < numPingThread; ++i )
        {
            worker_threads_.create_thread(
                boost::bind( &ThreadFun, &io_service_ping_ )
            );
        }
    }

    void Shutdown()
    {
        completionPing_->Stop();

        io_service_ping_.stop();

        work_exec_.reset();
        io_service_exec_.stop();

        work_.reset();
        io_service_.stop();

        worker::commDescrPool->Shutdown();

        worker::requestSem->Reset();

        worker_threads_.join_all();
    }

    void Run()
    {
        string pidfilePath = common::Config::Instance().Get<string>( "pidfile" );
        if ( pidfilePath[0] != '/' )
        {
            pidfilePath = exeDir_ + '/' + pidfilePath;
        }
        common::Pidfile pidfile( pidfilePath.c_str() );

        UnblockSighandlerMask();

        if ( !isDaemon_ )
        {
            UserInteraction();
        }
        else
        {
            PLOG( "started" );

            sigset_t waitset;
            int sig;
            sigemptyset( &waitset );
            sigaddset( &waitset, SIGTERM );
            while( 1 )
            {
                int ret = sigwait( &waitset, &sig );
                if ( !ret )
                    break;
                PLOG_ERR( "main(): sigwait failed: " << strerror(errno) );
            }
        }
    }

private:
    void RunPrExecProcess()
    {
        pid_t pid = fork();

        if ( pid < 0 )
        {
            PLOG_ERR( "RunPrExecProcess: fork() failed: " << strerror(errno) );
            exit( pid );
        }
        else
        if ( pid == 0 )
        {
            std::string exePath( exeDir_ );
            exePath += "/prexec";

            int ret = execl( exePath.c_str(), "prexec", "--num_thread",
                             ( boost::lexical_cast<std::string>( numRequestThread_ ) ).c_str(),
                             "--exe_dir", exeDir_.c_str(),
                             isDaemon_ ? "--d" : " ",
                             uid_ != 0 ? "--u" : " ",
                             uid_ != 0 ? ( boost::lexical_cast<std::string>( uid_ ) ).c_str() : " ",
                             !cfgDir_.empty() ? "--c" : " ",
                             !cfgDir_.empty() ? cfgDir_.c_str() : " ",
                             !resourcesDir_.empty() ? "--r" : " ",
                             !resourcesDir_.empty() ? resourcesDir_.c_str() : " ",
                             NULL );

            if ( ret < 0 )
            {
                PLOG_ERR( "RunPrExecProcess: execl failed: " << strerror(errno) );
                kill( getppid(), SIGTERM );
            }
        }
        else
        if ( pid > 0 )
        {
            // wait while prexec completes initialization
            worker::prexecPid = pid;
            siginfo_t info;
            sigset_t waitset;
            sigemptyset( &waitset );
            sigaddset( &waitset, SIGUSR1 );

            // TODO: sigtaimedwait && kill( pid, 0 )
            while( ( sigwaitinfo( &waitset, &info ) <= 0 ) && ( info.si_pid != pid ) );
        }
    }

    void SetupPrExecIPC()
    {
        namespace ipc = boost::interprocess;

        ipc::shared_memory_object::remove( worker::SHMEM_NAME );

        try
        {
            sharedMemPool_.reset( new ipc::shared_memory_object( ipc::create_only, worker::SHMEM_NAME, ipc::read_write ) );

            size_t shmemSize = numRequestThread_ * worker::SHMEM_BLOCK_SIZE;
            sharedMemPool_->truncate( shmemSize );

            mappedRegion_.reset( new ipc::mapped_region( *sharedMemPool_.get(), ipc::read_write ) );
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "SetupPrExecIPC failed: " << e.what() );
            exit( 1 );
        }
    }

private:
    string exeDir_, cfgDir_, resourcesDir_;
    bool isDaemon_;
    uid_t uid_;
    unsigned int numRequestThread_;
    unsigned int numThread_;

    boost::thread_group worker_threads_;

    boost::asio::io_service io_service_;
    boost::asio::io_service io_service_exec_;
    boost::asio::io_service io_service_ping_;

    boost::shared_ptr<boost::asio::io_service::work> work_;
    boost::shared_ptr<boost::asio::io_service::work> work_exec_;

    boost::shared_ptr< worker::CommDescrPool > commDescrPool_;

    boost::shared_ptr< worker::ConnectionAcceptor > acceptor_;

    boost::shared_ptr< worker::JobCompletionPinger > completionPing_;
    boost::shared_ptr< worker::MasterPing > masterPing_;

    boost::shared_ptr< boost::interprocess::shared_memory_object > sharedMemPool_;
    boost::shared_ptr< boost::interprocess::mapped_region > mappedRegion_;
};

} // anonymous namespace

int main( int argc, char* argv[], char **envp )
{
    try
    {
        bool isDaemon = false;
        uid_t uid = 0;

        std::string exeDir = boost::filesystem::system_complete( argv[0] ).branch_path().string();
        std::string cfgDir, resourcesDir;

        // parse input command line options
        namespace po = boost::program_options;
        
        po::options_description descr;

        descr.add_options()
            ("help", "Print help")
            ("d", "Run as a daemon")
            ("s", "Stop daemon")
            ("u", po::value<uid_t>(), "Start as a specific non-root user")
            ("c", po::value<std::string>(), "Config file path")
            ("r", po::value<std::string>(), "Path to resources");

        po::variables_map vm;
        po::store( po::parse_command_line( argc, argv, descr ), vm );
        po::notify( vm );

        if ( vm.count( "help" ) )
        {
            cout << descr << endl;
            return 1;
        }

        if ( vm.count( "s" ) )
        {
            return common::StopDaemon( "pworker" );
        }

        if ( vm.count( "c" ) )
        {
            cfgDir = vm[ "c" ].as<std::string>();
        }

        if ( vm.count( "r" ) )
        {
            resourcesDir = vm[ "r" ].as<std::string>();
        }

        if ( vm.count( "u" ) )
        {
            uid = vm[ "u" ].as<uid_t>();
        }
        VerifyCommandlineParams( uid );

        if ( vm.count( "d" ) )
        {
            common::StartAsDaemon();
            isDaemon = true;
        }

        WorkerApplication app( exeDir, cfgDir, resourcesDir, isDaemon, uid );
        app.Initialize();

        app.Run();

        app.Shutdown();
    }
    catch( std::exception &e )
    {
        cout << "Exception: " << e.what() << endl;
        PLOG_ERR( "Exception: " << e.what() );
    }

    PLOG( "stopped" );

    return 0;
}

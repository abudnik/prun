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
#include <thread>
#include <boost/program_options.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/make_shared.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/noncopyable.hpp>
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
#include "worker_job.h"
#include "job_parser.h"
#include "exec_info.h"
#include "job_completion_ping.h"
#include "computer_info.h"
#ifdef HAVE_EXEC_INFO_H
#include "common/stack.h"
#endif


using namespace std;
using boost::asio::ip::tcp;
using boost::asio::local::stream_protocol;

namespace worker {

pid_t g_prexecPid;


class ExecContext
{
private:
    void SetCommDescrPool( std::shared_ptr< worker::CommDescrPool > &pool )
    {
        commDescrPool_ = pool;
    }

public:
    ExecTable &GetExecTable() { return execTable_; }
    ExecTable &GetPendingTable() { return pendingTable_; }

    std::shared_ptr< worker::CommDescrPool > &GetCommDescrPool()
    {
        return commDescrPool_;
    }

private:
    ExecTable execTable_;
    ExecTable pendingTable_;

    std::shared_ptr< worker::CommDescrPool > commDescrPool_;

    friend class WorkerApplication;
};

typedef std::shared_ptr< ExecContext > ExecContextPtr;


class IAction
{
public:
    virtual ~IAction() {}
    virtual void Execute( const std::shared_ptr< Job > &job,
                          ExecContextPtr &execContext ) = 0;
};

class NoAction : public IAction
{
    virtual void Execute( const std::shared_ptr< Job > &job,
                          ExecContextPtr &execContex ) {}
};

class PrExecConnection
{
    typedef boost::array< char, 2048 > BufferType;

public:
    typedef PrExecConnection *connection_ptr;

public:
    PrExecConnection( ExecContextPtr &execContext )
    : socket_( nullptr ),
     response_( true ),
     execContext_( execContext )
    {}

    bool Init()
    {
        std::shared_ptr< CommDescrPool > &commDescrPool(
            execContext_->GetCommDescrPool()
        );
        if ( commDescrPool->AllocCommDescr() )
        {
            CommDescr &commDescr = commDescrPool->GetCommDescr();
            socket_ = commDescr.socket.get();
            buffer_.fill( 0 );
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
            std::shared_ptr< worker::CommDescrPool > &commDescrPool(
                execContext_->GetCommDescrPool()
            );
            commDescrPool->FreeCommDescr();
            socket_ = nullptr;
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
            boost::asio::write( *socket_, boost::asio::buffer( message ), boost::asio::transfer_all() );

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
    void HandleResponse()
    {
        std::istringstream ss( response_.GetString() );

        boost::property_tree::read_json( ss, responsePtree_ );
        errCode_ = responsePtree_.get<int>( "err" );
    }

private:
    stream_protocol::socket *socket_;
    BufferType buffer_;
    boost::property_tree::ptree responsePtree_;
    common::Request< BufferType > response_;
    ExecContextPtr &execContext_;
    int errCode_;
};

class PrExecConnectionHolder : private boost::noncopyable
{
public:
    PrExecConnectionHolder( PrExecConnection::connection_ptr con )
    : connection_( con )
    {}

    ~PrExecConnectionHolder()
    {
        try
        {
            connection_->Release();
        }
        catch( ... )
        {
            PLOG_ERR( "PrExecConnectionHolder::~PrExecConnectionHolder: caught unexpected exception" );
        }
    }

private:
    PrExecConnection::connection_ptr connection_;
};


class ExecuteTask : public IAction
{
    virtual void Execute( const std::shared_ptr< Job > &j,
                          ExecContextPtr &execContext )
    {
        std::shared_ptr< JobExec > job(
            std::dynamic_pointer_cast< JobExec >( j )
        );

        const JobExec::Tasks &tasks = job->GetTasks();
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

        const std::string &script = job->GetScript();
        if ( script.size() > SHMEM_BLOCK_SIZE )
        {
            PLOG_WRN( "ExecuteTask::Execute: script size=" << script.size() <<
                      ", exceeded limit=" << SHMEM_BLOCK_SIZE );
            job->OnError( NODE_SCRIPT_FILE_NOT_FOUND );
            SaveCompletionResults( job );
            return;
        }

        std::shared_ptr< CommDescrPool > &commDescrPool(
            execContext->GetCommDescrPool()
        );
        boost::asio::io_service *io_service = commDescrPool->GetIoService();
        for( auto taskId : tasks )
        {
            ExecInfo execInfo;
            execInfo.jobId_ = job->GetJobId();
            execInfo.taskId_ = taskId;
            execInfo.masterId_ = job->GetMasterId();

            ExecTable &pendingTable = execContext->GetPendingTable();
            pendingTable.Add( execInfo );

            io_service->post( boost::bind( &ExecuteTask::DoSend,
                                           boost::make_shared< ExecuteTask >(),
                                           std::shared_ptr< JobExec >( job ), taskId,
                                           ExecContextPtr( execContext ) ) );
        }
    }

    void SaveCompletionResults( const std::shared_ptr< JobExec > &job, int taskId, int64_t execTime ) const
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

    void SaveCompletionResults( const std::shared_ptr< JobExec > &job ) const
    {
        JobDescriptor descr;
        JobCompletionStat stat;
        descr.jobId = job->GetJobId();
        descr.masterIP = job->GetMasterIP();
        descr.masterId = job->GetMasterId();
        stat.errCode = job->GetErrorCode();

        const JobExec::Tasks &tasks = job->GetTasks();
        for( auto taskId : tasks )
        {
            descr.taskId = taskId;
            JobCompletionTable::Instance().Set( descr, stat );
        }
    }

    void NodeJobCompletionPing( const std::shared_ptr< JobExec > &job, int taskId,
                                ExecContextPtr &execContext )
    {
        std::shared_ptr< CommDescrPool > &commDescrPool(
            execContext->GetCommDescrPool()
        );
        boost::asio::io_service *io_service = commDescrPool->GetIoService();

        const common::Config &cfg = common::Config::Instance();
        const unsigned short master_ping_port = cfg.Get<unsigned short>( "master_ping_port" );

        using boost::asio::ip::udp;
        boost::asio::ip::address address( boost::asio::ip::address::from_string( job->GetMasterIP() ) );
        udp::endpoint master_endpoint( address, master_ping_port );
        udp::socket socket( *io_service, udp::endpoint( master_endpoint.protocol(), 0 ) );

        common::Marshaller marshaller;
        marshaller( "job_id", job->GetJobId() )
            ( "task_id", taskId );

        std::string msg;
        common::ProtocolJson protocol;
        protocol.Serialize( msg, "job_completion", marshaller );

        try
        {
            socket.send_to( boost::asio::buffer( msg ), master_endpoint );
        }
        catch( boost::system::system_error &e )
        {
            PLOG_ERR( "ExecuteTask::NodeJobCompletionPing: send_to failed: " << e.what() << ", host : " << master_endpoint );
        }
    }

    bool ExpandFilePath( const std::shared_ptr< JobExec > &job )
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
    void DoSend( const std::shared_ptr< JobExec > &job, int taskId,
                 ExecContextPtr &execContext )
    {
        PrExecConnection prExecConnection( execContext );
        PrExecConnectionHolder connectionHolder( &prExecConnection );
        if ( !prExecConnection.Init() )
            return;

        ExecInfo execInfo;
        execInfo.jobId_ = job->GetJobId();
        execInfo.taskId_ = taskId;
        execInfo.masterId_ = job->GetMasterId();

        ExecTable &execTable = execContext->GetExecTable();
        execTable.Add( execInfo );

        // write script into shared memory
        std::shared_ptr< CommDescrPool > &commDescrPool(
            execContext->GetCommDescrPool()
        );
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

        ExecTable &pendingTable = execContext->GetPendingTable();
        if ( pendingTable.Delete( job->GetJobId(), taskId, job->GetMasterId() ) )
        {
            errCode = prExecConnection.Send( ss2.str() );
        }
        else
        {
            errCode = NODE_JOB_TIMEOUT;
        }
        job->OnError( errCode );

        prExecConnection.Release();

        // save completion results and ping master
        execTable.Delete( job->GetJobId(), taskId, job->GetMasterId() );
        const boost::property_tree::ptree &responsePtree = prExecConnection.GetResponsePtree();
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

        NodeJobCompletionPing( job, taskId, execContext );
    }
};

class StopTask : public IAction
{
    virtual void Execute( const std::shared_ptr< Job > &j,
                          ExecContextPtr &execContext )
    {
        std::shared_ptr< JobStopTask > job(
            std::dynamic_pointer_cast< JobStopTask >( j )
        );

        ExecTable &pendingTable = execContext->GetPendingTable();
        if ( pendingTable.Delete( job->GetJobId(), job->GetTaskId(), job->GetMasterId() ) )
        {
            job->OnError( NODE_JOB_TIMEOUT );
        }

        ExecTable &execTable = execContext->GetExecTable();
        if ( !execTable.Contains( job->GetJobId(), job->GetTaskId(), job->GetMasterId() ) )
        {
            job->OnError( NODE_TASK_NOT_FOUND );
            return;
        }

        PrExecConnection prExecConnection( execContext );
        PrExecConnectionHolder connectionHolder( &prExecConnection );
        if ( !prExecConnection.Init() )
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

        int errCode = prExecConnection.Send( ss2.str() );
        job->OnError( errCode );
    }
};

class StopPreviousJobs : public IAction
{
    virtual void Execute( const std::shared_ptr< Job > &j,
                          ExecContextPtr &execContext )
    {
        std::shared_ptr< JobStopPreviousTask > job(
            std::dynamic_pointer_cast< JobStopPreviousTask >( j )
        );

        PrExecConnection prExecConnection( execContext );
        PrExecConnectionHolder connectionHolder( &prExecConnection );
        if ( !prExecConnection.Init() )
            return;

        // prepare json command
        boost::property_tree::ptree ptree;
        std::ostringstream ss, ss2;

        ptree.put( "task", "stop_prev" );
        ptree.put( "master_id", job->GetMasterId() );

        boost::property_tree::write_json( ss, ptree, false );

        ss2 << ss.str().size() << '\n' << ss.str();

        int errCode = prExecConnection.Send( ss2.str() );
        job->OnError( errCode );
    }
};

class StopAllJobs : public IAction
{
    virtual void Execute( const std::shared_ptr< Job > &j,
                          ExecContextPtr &execContext )
    {
        std::shared_ptr< JobStopAll > job(
            std::dynamic_pointer_cast< JobStopAll >( j )
        );

        ExecTable &pendingTable = execContext->GetPendingTable();
        pendingTable.Clear();

        PrExecConnection prExecConnection( execContext );
        PrExecConnectionHolder connectionHolder( &prExecConnection );
        if ( !prExecConnection.Init() )
            return;

        // prepare json command
        boost::property_tree::ptree ptree;
        std::ostringstream ss, ss2;

        ptree.put( "task", "stop_all" );

        boost::property_tree::write_json( ss, ptree, false );

        ss2 << ss.str().size() << '\n' << ss.str();

        int errCode = prExecConnection.Send( ss2.str() );
        job->OnError( errCode );
    }
};

class ActionCreator
{
public:
    virtual IAction *Create( const std::string &taskType )
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
        return nullptr;
    }
};

class Session
{
public:
    Session( std::shared_ptr< common::Semaphore > requestSem,
             ExecContextPtr &execContext )
    : requestSem_( requestSem ),
     execContext_( execContext )
    {}

    virtual ~Session()
    {
        requestSem_->Notify();
        cout << "S: ~Session()" << endl;
    }

protected:
    template< typename T >
    void HandleRequest( T &request )
    {
        RequestParser parser;
        if ( parser.ParseRequest( request, job_ ) )
        {
            job_->SetMasterIP( masterIP_ );

            ActionCreator actionCreator;
            std::unique_ptr< IAction > action(
                actionCreator.Create( job_->GetTaskType() )
            );
            if ( action )
            {
                action->Execute( job_, execContext_ );
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
        }
    }

    void SetMasterIP( const std::string &ip ) { masterIP_ = ip; }

protected:
    std::shared_ptr< Job > job_;
    std::string masterIP_;
    std::shared_ptr< common::Semaphore > requestSem_;
    ExecContextPtr execContext_;
};

class BoostSession : public Session, public boost::enable_shared_from_this< BoostSession >
{
public:
    typedef boost::array< char, 32 * 1024 > BufferType;

public:
    BoostSession( boost::asio::io_service &io_service,
                  std::shared_ptr< common::Semaphore > &requestSem,
                  ExecContextPtr &execContext )
    : Session( requestSem, execContext ),
     socket_( io_service ),
     request_( false )
    {}

    void Start()
    {
        boost::asio::ip::address remoteAddress = socket_.remote_endpoint().address();
        SetMasterIP( remoteAddress.to_string() );

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
        if ( !job_ )
            return;
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
    ConnectionAcceptor( boost::asio::io_service &io_service, unsigned short port,
                        std::shared_ptr< common::Semaphore > &requestSem,
                        ExecContextPtr &execContext )
    : io_service_( io_service ),
     acceptor_( io_service ),
     requestSem_( requestSem ),
     execContext_( execContext )
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
        session_ptr session( new BoostSession( io_service_, requestSem_, execContext_ ) );
        acceptor_.async_accept( session->GetSocket(),
                                boost::bind( &ConnectionAcceptor::HandleAccept, this,
                                             session, boost::asio::placeholders::error ) );
    }

    void HandleAccept( session_ptr session, const boost::system::error_code &error )
    {
        if ( !error )
        {
            cout << "connection accepted..." << endl;
            requestSem_->Wait();
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
    std::shared_ptr< common::Semaphore > requestSem_;
    ExecContextPtr execContext_;
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
    switch( s )
    {
        case SIGCHLD:
            int status;
            waitpid( -1, &status, WNOHANG );
            break;

        case SIGABRT:
        case SIGFPE:
        case SIGBUS:
        case SIGSEGV:
        case SIGILL:
        case SIGSYS:
        case SIGXCPU:
        case SIGXFSZ:
        {
#ifdef HAVE_EXEC_INFO_H
            std::ostringstream ss;
            common::Stack stack;
            stack.Out( ss );
            PLOG_ERR( "Signal '" << strsignal( s ) << "', current stack:" << std::endl << ss.str() );
#else
            PLOG_ERR( "Signal '" << strsignal( s ) << "'" );
#endif
            ::exit( 1 );
        }

        default:
            PLOG_WRN( "Unsupported signal '" << strsignal( s ) << "'" );
    }
}

void SetupSignalHandlers()
{
    struct sigaction sigHandler;
    memset( &sigHandler, 0, sizeof( sigHandler ) );
    sigHandler.sa_handler = SigHandler;
    sigemptyset(&sigHandler.sa_mask);
    sigHandler.sa_flags = 0;

    sigaction( SIGCHLD, &sigHandler, nullptr );

    sigaction( SIGABRT, &sigHandler, nullptr );
    sigaction( SIGFPE,  &sigHandler, nullptr );
    sigaction( SIGBUS,  &sigHandler, nullptr );
    sigaction( SIGSEGV, &sigHandler, nullptr );
    sigaction( SIGILL,  &sigHandler, nullptr );
    sigaction( SIGSYS,  &sigHandler, nullptr );
    sigaction( SIGXCPU, &sigHandler, nullptr );
    sigaction( SIGXFSZ, &sigHandler, nullptr );
}

void SetupSignalMask()
{
    sigset_t sigset;
    sigemptyset( &sigset );
    sigaddset( &sigset, SIGTERM );
    sigaddset( &sigset, SIGHUP );
    sigaddset( &sigset, SIGCHLD );
    sigaddset( &sigset, SIGUSR1 );
    sigprocmask( SIG_BLOCK, &sigset, nullptr );
}

void UnblockSighandlerMask()
{
    sigset_t sigset;
    // appropriate unblocking signals see in SetupSignalHandlers
    sigemptyset( &sigset );
    sigaddset( &sigset, SIGCHLD );
    pthread_sigmask( SIG_UNBLOCK, &sigset, nullptr );
}

void UserInteraction()
{
    while( !getchar() );
}


void AtExit()
{
    namespace ipc = boost::interprocess;

    // send stop signal to PrExec proccess
    kill( worker::g_prexecPid, SIGTERM );

    // remove shared memory
    ipc::shared_memory_object::remove( worker::SHMEM_NAME );

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

} // anonymous namespace


namespace worker {

class WorkerApplication
{
public:
    WorkerApplication( const std::string &exeDir, const std::string &cfgDir,
                       const std::string &resourcesDir, bool isDaemon, uid_t uid )
    : exeDir_( exeDir ),
     cfgDir_( cfgDir ),
     resourcesDir_( resourcesDir ),
     isDaemon_( isDaemon ), uid_( uid ),
     execContext_( new ExecContext )
    {}

    void Initialize()
    {
        ComputerInfo &compInfo = ComputerInfo::Instance();
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
        ApplyDefaults( cfg );

        JobCompletionTable::Instance();

        SetupSignalHandlers();
        SetupSignalMask();
        atexit( AtExit );

        SetupPrExecIPC();
        RunPrExecProcess();

        // create master request handlers
        work_.reset( new boost::asio::io_service::work( io_service_ ) );

        // create thread pool
        for( unsigned int i = 0; i < numThread_; ++i )
        {
            worker_threads_.push_back( std::thread( ThreadFun, &io_service_ ) );
        }

        // create pool for execute task actions
        work_exec_.reset( new boost::asio::io_service::work( io_service_exec_ ) );

        commDescrPool_.reset( new CommDescrPool( numRequestThread_, &io_service_exec_,
                                                 static_cast<char*>( mappedRegion_->get_address() ) )
        );
        execContext_->SetCommDescrPool( commDescrPool_ );

        for( unsigned int i = 0; i < numExecThread; ++i )
        {
            worker_threads_.push_back( std::thread( ThreadFun, &io_service_exec_ ) );
        }

        // start acceptor
        requestSem_.reset( new common::Semaphore( numRequestThread_ ) );
        const unsigned short port = cfg.Get<unsigned short>( "port" );
        acceptor_.reset(
            new ConnectionAcceptor( io_service_, port, requestSem_, execContext_ )
        );

        // create master ping handlers
        const int completionPingDelay = cfg.Get<int>( "completion_ping_delay" );
        completionPing_.reset( new JobCompletionPingerBoost( io_service_ping_, completionPingDelay ) );
        completionPing_->StartPing();

        masterPing_.reset( new MasterPingBoost( io_service_ping_ ) );
        masterPing_->Start();

        // create thread pool for pingers
        // 1. job completion pinger
        // 2. master heartbeat ping receiver
        unsigned int numPingThread = 2;
        for( unsigned int i = 0; i < numPingThread; ++i )
        {
            worker_threads_.push_back( std::thread( ThreadFun, &io_service_ping_ ) );
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

        commDescrPool_->Shutdown();

        requestSem_->Reset();

        for( auto &t : worker_threads_ )
            t.join();
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
                if ( ret == EINTR )
                    continue;
                if ( !ret )
                    break;
                PLOG_ERR( "main(): sigwait failed: " << strerror(ret) );
            }
        }
    }

private:
    void ApplyDefaults( common::Config &cfg ) const
    {
        cfg.Insert( "port", DEFAULT_PORT );
        cfg.Insert( "ping_port", DEFAULT_UDP_PORT );
        cfg.Insert( "master_ping_port", DEFAULT_MASTER_UDP_PORT );
    }

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
                             ( std::to_string( numRequestThread_ ) ).c_str(),
                             "--exe_dir", exeDir_.c_str(),
                             isDaemon_ ? "--d" : " ",
                             uid_ != 0 ? "--u" : " ",
                             uid_ != 0 ? ( std::to_string( uid_ ) ).c_str() : " ",
                             !cfgDir_.empty() ? "--c" : " ",
                             !cfgDir_.empty() ? cfgDir_.c_str() : " ",
                             !resourcesDir_.empty() ? "--r" : " ",
                             !resourcesDir_.empty() ? resourcesDir_.c_str() : " ",
                             nullptr );

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
            g_prexecPid = pid;
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

        ipc::shared_memory_object::remove( SHMEM_NAME );

        try
        {
            sharedMemPool_.reset( new ipc::shared_memory_object( ipc::create_only, SHMEM_NAME, ipc::read_write ) );

            size_t shmemSize = numRequestThread_ * SHMEM_BLOCK_SIZE;
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

    std::vector<std::thread> worker_threads_;

    boost::asio::io_service io_service_;
    boost::asio::io_service io_service_exec_;
    boost::asio::io_service io_service_ping_;

    std::shared_ptr<boost::asio::io_service::work> work_;
    std::shared_ptr<boost::asio::io_service::work> work_exec_;

    std::shared_ptr< CommDescrPool > commDescrPool_;

    std::shared_ptr< common::Semaphore > requestSem_;
    std::shared_ptr< ConnectionAcceptor > acceptor_;

    std::shared_ptr< JobCompletionPinger > completionPing_;
    std::shared_ptr< MasterPing > masterPing_;

    std::shared_ptr< boost::interprocess::shared_memory_object > sharedMemPool_;
    std::shared_ptr< boost::interprocess::mapped_region > mappedRegion_;

    ExecContextPtr execContext_;
};

} // namespace worker

int main( int argc, char* argv[] )
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

        worker::WorkerApplication app( exeDir, cfgDir, resourcesDir, isDaemon, uid );
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

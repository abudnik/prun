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
#include <boost/array.hpp>
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

namespace python_server {

bool isDaemon;
uid_t uid;
unsigned int numJobThreads;
unsigned int numThread;
pid_t pyexecPid;
string exeDir;

boost::interprocess::shared_memory_object *sharedMemPool;
boost::interprocess::mapped_region *mappedRegion;

Semaphore *taskSem;

CommDescrPool *commDescrPool;

ExecTable execTable;


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

class PyExecConnection : public boost::enable_shared_from_this< PyExecConnection >
{
    typedef boost::array< char, 1024 > BufferType;

public:
    typedef boost::shared_ptr< PyExecConnection > connection_ptr;

public:
    PyExecConnection()
    : response_( true )
    {
        CommDescr &commDescr = commDescrPool->GetCommDescr();
        socket_ = commDescr.socket.get();
        memset( buffer_.c_array(), 0, buffer_.size() );
    }

    int Send( const std::string &message )
    {
        errCode_ = 0;

        completed_ = false;

        try
        {
            socket_->async_read_some( boost::asio::buffer( buffer_ ),
                                     boost::bind( &PyExecConnection::FirstRead, shared_from_this(),
                                                  boost::asio::placeholders::error,
                                                  boost::asio::placeholders::bytes_transferred ) );

            boost::asio::async_write( *socket_,
                                      boost::asio::buffer( message ),
                                      boost::bind( &PyExecConnection::HandleWrite, shared_from_this(),
                                                   boost::asio::placeholders::error,
                                                   boost::asio::placeholders::bytes_transferred ) );

            boost::unique_lock< boost::mutex > lock( responseMut_ );
            while( !completed_ )
            {
                const boost::system_time timeout = boost::get_system_time() + boost::posix_time::milliseconds( RESPONSE_TIMEOUT );
                if ( responseCond_.timed_wait( lock, timeout ) )
                {
                    break;
                }
                else
                {
                    PS_LOG( "PyExecConnection::Send(): waiting task completion" );
                }
            }
        }
        catch( boost::system::system_error &e )
        {
            PS_LOG( "PyExecConnection::Send() failed: " << e.what() );
            errCode_ = NODE_FATAL;
        }

        return errCode_;
    }

    void Cancel()
    {
        errCode_ = NODE_FATAL;
        NotifyResponseCondVar();
    }

private:
    void HandleWrite( const boost::system::error_code& error, size_t bytes_transferred )
    {
        if ( error )
        {
            PS_LOG( "PyExecConnection::HandleWrite error=" << error.message() );
        }
    }

    void FirstRead( const boost::system::error_code& error, size_t bytes_transferred )
    {
        if ( error )
        {
            PS_LOG( "PyExecConnection::HandleRead error=" << error.message() );
        }
        else
        {
            int ret = response_.OnFirstRead( buffer_, bytes_transferred );
            if ( ret == 0 )
            {
                socket_->async_read_some( boost::asio::buffer( buffer_ ),
                                          boost::bind( &PyExecConnection::FirstRead, shared_from_this(),
                                                       boost::asio::placeholders::error,
                                                       boost::asio::placeholders::bytes_transferred ) );
                return;
            }
            if ( ret < 0 )
            {
                errCode_ = NODE_FATAL;
                NotifyResponseCondVar();
                return;
            }
        }

        HandleRead( error, bytes_transferred );
    }

    void HandleRead( const boost::system::error_code& error, size_t bytes_transferred )
    {
        if ( !error )
        {
            response_.OnRead( buffer_, bytes_transferred );

            if ( !response_.IsReadCompleted() )
            {
                socket_->async_read_some( boost::asio::buffer( buffer_ ),
                                          boost::bind( &PyExecConnection::HandleRead, shared_from_this(),
                                                       boost::asio::placeholders::error,
                                                       boost::asio::placeholders::bytes_transferred ) );
            }
            else
            {
                HandleResponse();
                NotifyResponseCondVar();
            }
        }
        else
        {
            PS_LOG( "PyExecConnection::HandleRead error=" << error.message() );
            errCode_ = NODE_FATAL;
            NotifyResponseCondVar();
        }
    }

    void HandleResponse()
    {
        std::istringstream ss( response_.GetString() );

        boost::property_tree::ptree ptree;
        boost::property_tree::read_json( ss, ptree );
        errCode_ = ptree.get<int>( "err" );
    }

    void NotifyResponseCondVar()
    {
        completed_ = true;
        boost::unique_lock< boost::mutex > lock( responseMut_ );
        responseCond_.notify_one();
    }

private:
    tcp::socket *socket_;
    BufferType buffer_;
    Request< BufferType > response_;
    bool completed_;
    boost::condition_variable responseCond_;
    boost::mutex responseMut_;
    int errCode_;
    const static int RESPONSE_TIMEOUT = 60 * 1000; // 60 sec
};

class ExecuteTask : public Action
{
    virtual void Execute( const boost::shared_ptr< Job > &job )
    {
        const Job::Tasks &tasks = job->GetTasks();
        if ( tasks.empty() )
        {
            PS_LOG( "ExecuteTask::Execute: empty tasks for jobId=" << job->GetJobId() );
            job->OnError( NODE_FATAL );
            return;
        }

        boost::asio::io_service *io_service = commDescrPool->GetIoService();
        Job::Tasks::const_iterator it = tasks.begin();

        int firstTaskId = *it;
        for( ++it; it != tasks.end(); ++it )
        {
            io_service->post( boost::bind( &ExecuteTask::DoSend,
                                           boost::shared_ptr< ExecuteTask >( new ExecuteTask ),
                                           boost::shared_ptr< Job >( job ), *it ) );
        }

        DoSend( job, firstTaskId );
    }

    void SaveCompletionResults( const boost::shared_ptr< Job > &job, int taskId ) const
    {
        JobDescriptor descr;
        JobCompletionStat stat;
        descr.jobId = job->GetJobId();
        descr.taskId = taskId;
        descr.masterIP = job->GetMasterIP();
        stat.errCode = job->GetErrorCode();
        JobCompletionTable::Instance().Set( descr, stat );
    }

    void NodeJobCompletionPing( const boost::shared_ptr< Job > &job, int taskId )
    {
        boost::asio::io_service *io_service = commDescrPool->GetIoService();

        using boost::asio::ip::udp;
        udp::socket socket( *io_service, udp::endpoint( udp::v4(), 0 ) );
        udp::endpoint master_endpoint( boost::asio::ip::address::from_string( job->GetMasterIP() ), DEFAULT_MASTER_UDP_PORT );

        ProtocolJson protocol;
        std::string msg;
        protocol.NodeJobCompletionPing( msg, job->GetJobId(), taskId );

        try
        {
            socket.send_to( boost::asio::buffer( msg ), master_endpoint );
        }
        catch( boost::system::system_error &e )
        {
            PS_LOG( "ExecuteTask::NodeJobCompletionPing: send_to failed: " << e.what() << ", host : " << master_endpoint );
        }
    }

public:
    void DoSend( const boost::shared_ptr< Job > &job, int taskId )
    {
        commDescrPool->AllocCommDescr();
        PyExecConnection::connection_ptr pyExecConnection( new PyExecConnection() );

        ExecInfo execInfo;
        execInfo.jobId_ = job->GetJobId();
        execInfo.taskId_ = taskId;
        execInfo.callback_ = boost::bind( &PyExecConnection::Cancel, pyExecConnection );
        execTable.Add( execInfo );

        // write script into shared memory
        const std::string &script = job->GetScript();
        CommDescr &commDescr = commDescrPool->GetCommDescr();
        memcpy( commDescr.shmemAddr, script.c_str(), script.size() );
        char *shmemRequestEnd = commDescr.shmemAddr + script.size();
        *shmemRequestEnd = '\0';

        // prepare json command
        boost::property_tree::ptree ptree;
        std::ostringstream ss, ss2;

        ptree.put( "task", "exec" );
        ptree.put( "id", commDescr.shmemBlockId );
        ptree.put( "len", script.size() );
        ptree.put( "lang", job->GetScriptLanguage() );
        ptree.put( "job_id", job->GetJobId() );
        ptree.put( "task_id", taskId );
        ptree.put( "num_tasks", job->GetNumTasks() );
        ptree.put( "timeout", job->GetTimeout() );

        boost::property_tree::write_json( ss, ptree, false );

        ss2 << ss.str().size() << '\n' << ss.str();

        int errCode = pyExecConnection->Send( ss2.str() );
        job->OnError( errCode );

        execTable.Delete( job->GetJobId(), taskId );

        commDescrPool->FreeCommDescr();

        SaveCompletionResults( job, taskId );
        NodeJobCompletionPing( job, taskId );
    }
};

class StopTask : public Action
{
    virtual void Execute( const boost::shared_ptr< Job > &job )
    {
        if ( !execTable.Contains( job->GetJobId(), job->GetTaskId() ) )
        {
            job->OnError( NODE_TASK_NOT_FOUND );
            return;
        }

        commDescrPool->AllocCommDescr();
        PyExecConnection::connection_ptr pyExecConnection( new PyExecConnection() );

        // prepare json command
        boost::property_tree::ptree ptree;
        std::ostringstream ss, ss2;

        ptree.put( "task", "stop_task" );
        ptree.put( "job_id", job->GetJobId() );
        ptree.put( "task_id", job->GetTaskId() );

        boost::property_tree::write_json( ss, ptree, false );

        ss2 << ss.str().size() << '\n' << ss.str();

        int errCode = pyExecConnection->Send( ss2.str() );
        job->OnError( errCode );

        commDescrPool->FreeCommDescr();
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
        return NULL;
    }
};

class Session
{
public:
    Session()
    : job_( new Job )
    {}

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
                PS_LOG( "Session::HandleRequest: appropriate action not found for task type: "
                        << job_->GetTaskType() );
                job_->OnError( NODE_FATAL );
            }
        }
        else
        {
            PS_LOG( request.GetString() );
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
     request_( false ),
     io_service_( io_service )
    {}

    virtual ~BoostSession()
    {
        taskSem->Notify();
        cout << "S: ~Session()" << endl;
    }

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
            PS_LOG( "SessionBoost::FirstRead error=" << error.message() );
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
            PS_LOG( "SessionBoost::HandleRead error=" << error.message() );
            //HandleError( error );
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
            PS_LOG( "SessionBoost::HandleWrite error=" << error.message() );
        }
    }

protected:
    tcp::socket socket_;
    BufferType buffer_;
    Request< BufferType > request_;
    char readStatus_;
    std::string response_;
    boost::asio::io_service &io_service_;
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
            tcp::endpoint endpoint( tcp::v4(), port );
            acceptor_.open( endpoint.protocol() );
            acceptor_.set_option( tcp::acceptor::reuse_address( true ) );
            acceptor_.set_option( tcp::no_delay( true ) );
            acceptor_.bind( endpoint );
            acceptor_.listen();
        }
        catch( std::exception &e )
        {
            PS_LOG( "ConnectionAcceptor: " << e.what() );
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
            taskSem->Wait();
            io_service_.post( boost::bind( &BoostSession::Start, session ) );
            StartAccept();
        }
        else
        {
            PS_LOG( "HandleAccept: " << error.message() );
        }
    }

private:
    boost::asio::io_service &io_service_;
    tcp::acceptor acceptor_;
};

} // namespace python_server


namespace {

void VerifyCommandlineParams()
{
    if ( python_server::uid )
    {
        // check uid existance
        char line[256] = { '\0' };

        std::ostringstream command;
        command << "getent passwd " << python_server::uid << "|cut -d: -f1";

        FILE *cmd = popen( command.str().c_str(), "r" );
        fgets( line, sizeof(line), cmd );
        pclose( cmd );

        if ( !strlen( line ) )
        {
            std::cout << "Unknown uid: " << python_server::uid << std::endl;
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
        pid_t pid = waitpid( python_server::pyexecPid, &status, WNOHANG );
        // if ( pid != python_server::pyexecPid )
        // {
        //  PS_LOG( "SigHandler: waitpid() failed, pid= " << python_server::pyexecPid << ", err= " << strerror(errno) );
        // }
        // else
        // {
        //  PS_LOG( "PyExec proccess stopped (" << status << ")" );
        // }
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

void RunPyExecProcess()
{
    pid_t pid = fork();

    if ( pid < 0 )
    {
        PS_LOG( "RunPyExecProcess: fork() failed: " << strerror(errno) );
        exit( pid );
    }
    else
    if ( pid == 0 )
    {
        std::string exePath( python_server::exeDir );
        exePath += "/pyexec";

        int ret = execl( exePath.c_str(), "pyexec", "--num_thread",
                         ( boost::lexical_cast<std::string>( python_server::numJobThreads ) ).c_str(),
                         "--exe_dir", python_server::exeDir.c_str(),
                         python_server::isDaemon ? "--d" : " ",
                         python_server::uid != 0 ? "--u" : " ",
                         python_server::uid != 0 ? ( boost::lexical_cast<std::string>( python_server::uid ) ).c_str() : " ",
                         NULL );

        if ( ret < 0 )
        {
            PS_LOG( "RunPyExecProcess: execl failed: " << strerror(errno) );
            kill( getppid(), SIGTERM );
        }
    }
    else
    if ( pid > 0 )
    {
        // wait while pyexec completes initialization
        python_server::pyexecPid = pid;
        siginfo_t info;
        sigset_t waitset;
        sigemptyset( &waitset );
        sigaddset( &waitset, SIGUSR1 );

        // TODO: sigtaimedwait && kill( pid, 0 )
        while( ( sigwaitinfo( &waitset, &info ) <= 0 ) && ( info.si_pid != pid ) );
    }
}

void SetupPyExecIPC()
{
    namespace ipc = boost::interprocess;

    ipc::shared_memory_object::remove( python_server::SHMEM_NAME );

    try
    {
        python_server::sharedMemPool = new ipc::shared_memory_object( ipc::create_only, python_server::SHMEM_NAME, ipc::read_write );

        size_t shmemSize = python_server::numJobThreads * python_server::SHMEM_BLOCK_SIZE;
        python_server::sharedMemPool->truncate( shmemSize );

        python_server::mappedRegion = new ipc::mapped_region( *python_server::sharedMemPool, ipc::read_write );
    }
    catch( std::exception &e )
    {
        PS_LOG( "SetupPyExecIPC failed: " << e.what() );
        exit( 1 );
    }
}

void AtExit()
{
    namespace ipc = boost::interprocess;

    // send stop signal to PyExec proccess
    kill( python_server::pyexecPid, SIGTERM );

    // remove shared memory
    ipc::shared_memory_object::remove( python_server::SHMEM_NAME );

    delete python_server::mappedRegion;
    python_server::mappedRegion = NULL;

    delete python_server::sharedMemPool;
    python_server::sharedMemPool = NULL;

    delete python_server::taskSem;
    python_server::taskSem = NULL;

    python_server::logger::ShutdownLogger();
}

void ThreadFun( boost::asio::io_service *io_service )
{
    try
    {
        io_service->run();
    }
    catch( std::exception &e )
    {
        PS_LOG( "ThreadFun: " << e.what() );
    }
}

} // anonymous namespace


int main( int argc, char* argv[], char **envp )
{
    try
    {
        // initialization
        python_server::ComputerInfo &compInfo = python_server::ComputerInfo::Instance();
        python_server::numJobThreads = 2 * compInfo.GetNumCPU();
        python_server::isDaemon = false;
        python_server::uid = 0;

        python_server::exeDir = boost::filesystem::system_complete( argv[0] ).branch_path().string();

        // parse input command line options
        namespace po = boost::program_options;
        
        po::options_description descr;

        descr.add_options()
            ("help", "Print help")
            ("num_thread", po::value<unsigned int>(), "Jobs thread pool size")
            ("d", "Run as a daemon")
            ("stop", "Stop daemon")
            ("u", po::value<uid_t>(), "Start as a specific non-root user");
        
        po::variables_map vm;
        po::store( po::parse_command_line( argc, argv, descr ), vm );
        po::notify( vm );

        if ( vm.count( "help" ) )
        {
            cout << descr << endl;
            return 1;
        }

        if ( vm.count( "stop" ) )
        {
            return python_server::StopDaemon( "pyserver" );
        }

        if ( vm.count( "u" ) )
        {
            python_server::uid = vm[ "u" ].as<uid_t>();
        }
        VerifyCommandlineParams();

        if ( vm.count( "d" ) )
        {
            python_server::StartAsDaemon();
            python_server::isDaemon = true;
        }

        if ( vm.count( "num_thread" ) )
        {
            python_server::numJobThreads = vm[ "num_thread" ].as<unsigned int>();
        }
        // 1. accept thread
        // 2. additional worker thread for async reading results from pyexec
        python_server::numThread = python_server::numJobThreads + 2;

        python_server::logger::InitLogger( python_server::isDaemon, "PythonServer" );

        python_server::Config::Instance().ParseConfig( python_server::exeDir.c_str() );

        string pidfilePath = python_server::Config::Instance().Get<string>( "pidfile" );
        if ( pidfilePath[0] != '/' )
        {
            pidfilePath = python_server::exeDir + '/' + pidfilePath;
        }
        python_server::Pidfile pidfile( pidfilePath.c_str() );

        python_server::JobCompletionTable::Instance();

        SetupSignalHandlers();
        SetupSignalMask();
        atexit( AtExit );

        SetupPyExecIPC();
        RunPyExecProcess();

        // start accepting client connections
        boost::asio::io_service io_service;
        boost::scoped_ptr<boost::asio::io_service::work> work(
            new boost::asio::io_service::work( io_service ) );

        boost::scoped_ptr< python_server::CommDescrPool > commDescrPool(
            new python_server::CommDescrPool( python_server::numJobThreads, &io_service,
                                              (char*)python_server::mappedRegion->get_address() ) );
        python_server::commDescrPool = commDescrPool.get();

        python_server::taskSem = new python_server::Semaphore( python_server::numJobThreads );

        // create thread pool
        boost::thread_group worker_threads;
        for( unsigned int i = 0; i < python_server::numThread; ++i )
        {
            worker_threads.create_thread(
                boost::bind( &ThreadFun, &io_service )
            );
        }

        python_server::ConnectionAcceptor acceptor( io_service, python_server::DEFAULT_PORT );

        boost::asio::io_service io_service_ping;

        int completionPingTimeout = python_server::Config::Instance().Get<int>( "completion_ping_timeout" );
        boost::scoped_ptr< python_server::JobCompletionPinger > completionPing(
            new python_server::JobCompletionPingerBoost( io_service_ping, completionPingTimeout ) );
        completionPing->StartPing();

        boost::scoped_ptr< python_server::MasterPing > masterPing(
            new python_server::MasterPingBoost( io_service_ping ) );
        masterPing->Start();

        // create thread pool for pingers
        // 1. job completion pinger
        // 2. master heartbeat ping receiver
        unsigned int numPingThread = 2;
        for( unsigned int i = 0; i < numPingThread; ++i )
        {
            worker_threads.create_thread(
                boost::bind( &ThreadFun, &io_service_ping )
            );
        }

        UnblockSighandlerMask();

        if ( !python_server::isDaemon )
        {
            UserInteraction();
        }
        else
        {
            PS_LOG( "started" );

            sigset_t waitset;
            int sig;
            sigemptyset( &waitset );
            sigaddset( &waitset, SIGTERM );
            while( 1 )
            {
                int ret = sigwait( &waitset, &sig );
                if ( !ret )
                    break;
                PS_LOG( "main(): sigwait failed: " << strerror(errno) );
            }
        }

        completionPing->Stop();

        io_service_ping.stop();

        work.reset();
        io_service.stop();

        python_server::taskSem->Notify();

        python_server::execTable.Clear();

        worker_threads.join_all();
    }
    catch( std::exception &e )
    {
        cout << "Exception: " << e.what() << endl;
        PS_LOG( "Exception: " << e.what() );
    }

    PS_LOG( "stopped" );

    return 0;
}

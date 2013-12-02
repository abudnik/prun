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
#include <boost/filesystem.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/thread.hpp>
#include <boost/array.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp> 
#include <unistd.h>
#include <csignal>
#include <sys/wait.h>
#include "common.h"
#include "common/request.h"
#include "common/log.h"
#include "common/config.h"
#include "common/error_code.h"
#include "common/configure.h"
#include "exec_info.h"
#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

using namespace std;
using boost::asio::ip::tcp;

namespace worker {

bool isDaemon;
bool isFork;
uid_t uid;
unsigned int numThread;
string exeDir;

boost::interprocess::shared_memory_object *sharedMemPool;
boost::interprocess::mapped_region *mappedRegion; 

struct ThreadParams
{
    int writeFifoFD, readFifoFD;
    string writeFifo, readFifo;
    pid_t pid;
};

typedef std::map< boost::thread::id, ThreadParams > ThreadInfo;
ThreadInfo threadInfo;

ExecTable execTable;

struct ChildProcesses
{
    void Add( pid_t pid )
    {
        boost::unique_lock< boost::mutex > lock( mut_ );
        pids_.insert( pid );
    }

    bool Delete( pid_t pid )
    {
        boost::unique_lock< boost::mutex > lock( mut_ );
        std::set< pid_t >::iterator it = pids_.find( pid );
        if ( it != pids_.end() )
        {
            pids_.erase( it );
            return true;
        }
        return false;
    }

private:
    std::set< pid_t > pids_;
    boost::mutex mut_;
};
ChildProcesses childProcesses;


class Job
{
public:
    Job()
    : errCode_( 0 )
    {}

    void OnError( int err ) { errCode_ = err; }
    int GetError() const { return errCode_; }

private:
    int errCode_;
};

class JobExec : public Job
{
public:
    void ParseRequest( boost::property_tree::ptree &ptree )
    {
        commId_ = ptree.get<int>( "id" );
        scriptLength_ = ptree.get<unsigned int>( "len" );
        language_ = ptree.get<std::string>( "lang" );
        filePath_ = ptree.get<std::string>( "path" );
        jobId_ = ptree.get<int64_t>( "job_id" );
        taskId_ = ptree.get<int>( "task_id" );
        masterId_ = ptree.get<std::string>( "master_id" );

        numTasks_ = ptree.get<int>( "num_tasks" );
        timeout_ = ptree.get<int>( "timeout" );

        fromFile_ = !scriptLength_;
    }

    void SetScriptLength( unsigned int len ) { scriptLength_ = len; }
    void SetExecTime( int64_t time ) { execTime_ = time; }

    int GetCommId() const { return commId_; }
    unsigned int GetScriptLength() const { return scriptLength_; }
    const std::string &GetScriptLanguage() const { return language_; }
    const std::string &GetFilePath() const { return filePath_; }
    int64_t GetJobId() const { return jobId_; }
    int GetTaskId() const { return taskId_; }
    const std::string &GetMasterId() const { return masterId_; }
    int GetNumTasks() const { return numTasks_; }
    int GetTimeout() const { return timeout_; }
    bool IsFromFile() const { return fromFile_; }
    int64_t GetExecTime() const { return execTime_; }

private:
    int commId_;
    unsigned int scriptLength_;
    std::string language_;
    std::string filePath_;
    int64_t jobId_;
    int taskId_;
    std::string masterId_;
    int numTasks_;
    int timeout_;
    bool fromFile_;
    int64_t execTime_;
};

class JobStopTask : public Job
{
public:
    JobStopTask() {}

    JobStopTask( int64_t jobId, int taskId, const std::string &masterId )
    : jobId_( jobId ), taskId_( taskId ), masterId_( masterId )
    {}

    void ParseRequest( boost::property_tree::ptree &ptree )
    {
        jobId_ = ptree.get<int64_t>( "job_id" );
        taskId_ = ptree.get<int>( "task_id" );
        masterId_ = ptree.get<std::string>( "master_id" );
    }

    int64_t GetJobId() const { return jobId_; }
    int GetTaskId() const { return taskId_; }
    const std::string &GetMasterId() const { return masterId_; }

private:
    int64_t jobId_;
    int taskId_;
    std::string masterId_;
};

class JobStopPreviousJobs : public Job
{
public:
    void ParseRequest( boost::property_tree::ptree &ptree )
    {
        masterId_ = ptree.get<std::string>( "master_id" );
    }

    const std::string &GetMasterId() const { return masterId_; }

private:
    std::string masterId_;
};

class StopTaskAction
{
public:
    void StopTask( JobStopTask &job )
    {
        ExecInfo execInfo;
        if ( execTable.Find( job.GetJobId(), job.GetTaskId(), job.GetMasterId(), execInfo ) &&
             execTable.Delete( job.GetJobId(), job.GetTaskId(), job.GetMasterId() ) )
        {
            pid_t pid = execInfo.pid_;

            if ( !childProcesses.Delete( pid ) )
            {
                PS_LOG( "StopTaskAction::StopTask: child process already terminated" );
                job.OnError( NODE_TASK_NOT_FOUND );
                return;
            }

            int ret = kill( pid, SIGTERM );
            if ( ret != -1 )
            {
                int fifo = FindFifo( pid );
                if ( fifo != -1 )
                {
                    int errCode = NODE_JOB_TIMEOUT;
                    int ret = write( fifo, &errCode, sizeof( errCode ) );
                    if ( ret == -1 )
                        PS_LOG( "StopTaskAction::StopTask: write fifo failed, err=" << strerror(errno) );
                }
                else
                {
                    PS_LOG( "StopTaskAction::StopTask: fifo not found for pid=" << pid );
                }

                PS_LOG( "StopTaskAction::StopTask: task stopped, pid=" << pid <<
                        ", jobId=" << job.GetJobId() << ", taskId=" << job.GetTaskId() );
            }
            else
            {
                PS_LOG( "StopTaskAction::StopTask: process killing failed: pid=" << pid << ", err=" << strerror(errno) );
                job.OnError( NODE_FATAL );
            }
        }
        else
        {
            PS_LOG( "StopTaskAction::StopTask: task not found, jobId=" << job.GetJobId() <<
                    ", taskId=" << job.GetTaskId() << ", " << "masterId=" << job.GetMasterId() );
            job.OnError( NODE_TASK_NOT_FOUND );
        }
    }

private:
    int FindFifo( pid_t pid )
    {
        ThreadInfo::const_iterator it = threadInfo.begin();
        for( ; it != threadInfo.end(); ++it )
        {
            const ThreadParams &threadParams = it->second;
            if ( threadParams.pid == pid )
                return threadParams.readFifoFD;
        }
        return -1;
    }
};

class StopPreviousJobsAction
{
public:
    void StopJobs( JobStopPreviousJobs &job )
    {
        std::list< ExecInfo > table;
        execTable.Get( table );
        std::list< ExecInfo >::const_iterator it = table.begin();
        for( ; it != table.end(); ++it )
        {
            const ExecInfo &execInfo = *it;
            if ( execInfo.masterId_ != job.GetMasterId() )
            {
                JobStopTask job( execInfo.jobId_, execInfo.taskId_, execInfo.masterId_ );
                StopTaskAction action;
                action.StopTask( job );
            }
        }
    }
};

class ScriptExec
{
public:
    virtual ~ScriptExec() {}

    virtual void Execute( JobExec *job )
    {
        if ( !InitLanguageEnv() )
        {
            job->OnError( NODE_FATAL );
            return;
        }

        job_ = job;

        pid_t pid = DoFork();
        if ( pid > 0 )
            return;

        string scriptLength = boost::lexical_cast<std::string>( job->GetScriptLength() );

        string taskId = boost::lexical_cast<std::string>( job->GetTaskId() );
        string numTasks = boost::lexical_cast<std::string>( job->GetNumTasks() );

        const ThreadParams &threadParams = threadInfo[ boost::this_thread::get_id() ];

        int ret = execl( exePath_.c_str(), job->GetScriptLanguage().c_str(),
                         nodePath_.c_str(),
                         threadParams.readFifo.c_str(), threadParams.writeFifo.c_str(),
                         scriptLength.c_str(),
                         taskId.c_str(), numTasks.c_str(), NULL );
        if ( ret < 0 )
        {
            PS_LOG( "ScriptExec::Execute: execl failed: " << strerror(errno) );
        }
        ::exit( 1 );
    }

    virtual void KillExec( pid_t pid )
    {
        PS_LOG( "poll timed out, trying to kill process: " << pid );

        if ( execTable.Delete( job_->GetJobId(), job_->GetTaskId(), job_->GetMasterId() ) &&
             childProcesses.Delete( pid ) )
        {
            int ret = kill( pid, SIGTERM );
            if ( ret == -1 )
            {
                PS_LOG( "process killing failed: pid=" << pid << ", err=" << strerror(errno) );
            }
        }
        else
        {
            PS_LOG( "ScriptExec::KillExec: it seems that process is already killed, pid=" << pid );
        }
    }

    static void Cancel( const ExecInfo &execInfo )
    {
        JobStopTask job( execInfo.jobId_, execInfo.taskId_, execInfo.masterId_ );
        StopTaskAction action;
        action.StopTask( job );
    }

protected:
    virtual bool InitLanguageEnv() = 0;

    virtual pid_t DoFork()
    {
        // flush fifo before fork, because after forking completion status of a
        // forked process may be lost
        FlushFifo();

        struct timeval tvStart, tvEnd;
        gettimeofday( &tvStart, NULL );

        pid_t pid = fork();

        if ( pid > 0 )
        {
            childProcesses.Add( pid );
            ThreadParams &threadParams = threadInfo[ boost::this_thread::get_id() ];
            threadParams.pid = pid;

            ExecInfo execInfo;
            execInfo.jobId_ = job_->GetJobId();
            execInfo.taskId_ = job_->GetTaskId();
            execInfo.masterId_ = job_->GetMasterId();
            execInfo.pid_ = pid;
            execInfo.callback_ = boost::bind( &ScriptExec::Cancel, execInfo );
            execTable.Add( execInfo );

            bool succeded = WriteScript( threadParams.writeFifoFD );
            if ( succeded )
            {
                succeded = ReadCompletionStatus( threadParams.readFifoFD );
            }

            gettimeofday( &tvEnd, NULL );
            int64_t elapsed = (int64_t)( tvEnd.tv_sec - tvStart.tv_sec ) * 1000 +
                                       ( tvEnd.tv_usec - tvStart.tv_usec ) / 1000;
            job_->SetExecTime( elapsed );

            if ( !succeded)
            {
                if ( job_->GetError() == NODE_JOB_TIMEOUT )
                    KillExec( pid );
            }

            execTable.Delete( job_->GetJobId(), job_->GetTaskId(), job_->GetMasterId() );
        }
        else
        if ( pid == 0 )
        {
            sigset_t sigset;
            sigemptyset( &sigset );
            sigaddset( &sigset, SIGTERM );
            pthread_sigmask( SIG_UNBLOCK, &sigset, NULL );

            isFork = true;
            // linux-only. kill child process, if parent exits
#ifdef HAVE_SYS_PRCTL_H
            prctl( PR_SET_PDEATHSIG, SIGHUP );
#endif

            if ( job_->IsFromFile() )
            {
                const std::string &scriptPath = job_->GetFilePath();
                boost::filesystem::path p( scriptPath );
                boost::filesystem::path dir = p.parent_path();
                int ret = chdir( dir.string().c_str() );
                if ( ret < 0 )
                {
                    PS_LOG( "ScriptExec::DoFork: chdir failed: " << strerror(errno) );
                }

                job_->SetScriptLength( boost::filesystem::file_size( p ) );
            }
        }
        else
        {
            PS_LOG( "ScriptExec::DoFork: fork() failed " << strerror(errno) );
        }

        return pid;
    }

    bool WriteScript( int fifo )
    {
        std::string scriptData;
        const char *scriptAddr = NULL;
        unsigned int bytesToWrite = 0;

        if ( job_->IsFromFile() )
        {
            // read script from file
            const std::string &filePath = job_->GetFilePath();
            std::ifstream file( filePath.c_str() );
            if ( file.is_open() )
            {
                file.seekg( 0, std::ios::end );
                scriptData.resize( file.tellg() );
                file.seekg( 0, std::ios::beg );
                file.read( &scriptData[0], scriptData.size() );
            }
            else
            {
                PS_LOG( "ScriptExec::WriteScript: couldn't open " << filePath );
            }
            scriptAddr = scriptData.c_str();
            bytesToWrite = scriptData.size();
        }
        else
        {
            // read script from shared memory
            size_t offset = job_->GetCommId() * SHMEM_BLOCK_SIZE;
            scriptAddr = (const char*)worker::mappedRegion->get_address() + offset;
            bytesToWrite = job_->GetScriptLength();
        }

        pollfd pfd[1];
        pfd[0].fd = fifo;
        pfd[0].events = POLLOUT;

        int timeout = job_->GetTimeout();
        if ( timeout > 0 )
            timeout *= 1000;

        int errCode = NODE_FATAL;
        int ret = poll( pfd, 1, timeout );
        if ( ret > 0 )
        {
            size_t offset = 0;
            while( bytesToWrite )
            {
                ret = write( fifo, scriptAddr + offset, bytesToWrite );
                if ( ret > 0 )
                {
                    offset += ret;
                    bytesToWrite -= ret;
                }
                else
                {
                    if ( errno == EAGAIN )
                        continue;
                    PS_LOG( "ScriptExec::WriteScript: write failed: " << strerror(errno) );
                    break;
                }
            }
            return !bytesToWrite;
        }
        else
        if ( ret == 0 )
        {
            errCode = NODE_JOB_TIMEOUT;
        }
        else
        {
            errCode = NODE_FATAL;
            PS_LOG( "ScriptExec::WriteScript: poll failed: " << strerror(errno) );
        }

        job_->OnError( errCode );
        return false;
    }

    bool ReadCompletionStatus( int fifo )
    {
        pollfd pfd[1];
        pfd[0].fd = fifo;
        pfd[0].events = POLLIN | POLLERR;

        int timeout = job_->GetTimeout();
        if ( timeout > 0 )
            timeout *= 1000;

        int errCode = NODE_FATAL;
        int ret = poll( pfd, 1, timeout );
        if ( ret > 0 )
        {
            ret = read( fifo, &errCode, sizeof( errCode ) );
            if ( ret > 0 )
            {
                job_->OnError( errCode );
                return true;
            }
            else
            {
                PS_LOG( "ScriptExec::ReadCompletionStatus: read fifo failed: " << strerror(errno) );
            }
        }
        else
        if ( ret == 0 )
        {
            errCode = NODE_JOB_TIMEOUT;
        }
        else
        {
            errCode = NODE_FATAL;
            PS_LOG( "ScriptExec::ReadCompletionStatus: poll failed: " << strerror(errno) );
        }

        job_->OnError( errCode );
        return false;
    }

private:
    void FlushFifo()
    {
        ThreadParams &threadParams = threadInfo[ boost::this_thread::get_id() ];
        int fifo = threadParams.readFifoFD;

        pollfd pfd[1];
        pfd[0].fd = fifo;
        pfd[0].events = POLLIN;
        while( 1 )
        {
            int ret = poll( pfd, 1, 0 );
            if ( ret > 0 )
            {
                char buf[64];
                ret = read( fifo, buf, sizeof( buf ) );
                if ( ret > 0 )
                    continue;
            }
            break;
        }
    }

protected:
    JobExec *job_;
    std::string exePath_;
    std::string nodePath_;
};

class PythonExec : public ScriptExec
{
protected:
    virtual bool InitLanguageEnv()
    {
        try
        {
            exePath_ = common::Config::Instance().Get<string>( "python" );
            nodePath_ = exeDir + '/' + NODE_SCRIPT_NAME_PY;
        }
        catch( std::exception &e )
        {
            PS_LOG( "PythonExec::Init: " << e.what() );
            return false;
        }
        return true;
    }
};

class JavaExec : public ScriptExec
{
public:
    virtual void Execute( JobExec *job )
    {
        if ( !InitLanguageEnv() )
        {
            job->OnError( NODE_FATAL );
            return;
        }

        job_ = job;

        pid_t pid = DoFork();
        if ( pid > 0 )
            return;

        string scriptLength = boost::lexical_cast<std::string>( job->GetScriptLength() );

        string taskId = boost::lexical_cast<std::string>( job->GetTaskId() );
        string numTasks = boost::lexical_cast<std::string>( job->GetNumTasks() );

        const ThreadParams &threadParams = threadInfo[ boost::this_thread::get_id() ];

        int ret = execl( exePath_.c_str(), job->GetScriptLanguage().c_str(),
                         "-cp", nodePath_.c_str(),
                         "node",
                         threadParams.readFifo.c_str(), threadParams.writeFifo.c_str(),
                         scriptLength.c_str(),
                         taskId.c_str(), numTasks.c_str(), NULL );
        if ( ret < 0 )
        {
            PS_LOG( "JavaExec::Execute: execl failed: " << strerror(errno) );
        }
        ::exit( 1 );
    }

protected:
    virtual bool InitLanguageEnv()
    {
        try
        {
            exePath_ = common::Config::Instance().Get<string>( "java" );
            nodePath_ = exeDir + "/node";
        }
        catch( std::exception &e )
        {
            PS_LOG( "JavaExec::Init: " << e.what() );
            return false;
        }
        return true;
    }
};

class ShellExec : public ScriptExec
{
protected:
    virtual bool InitLanguageEnv()
    {
        try
        {
            exePath_ = common::Config::Instance().Get<string>( "shell" );
            nodePath_ = exeDir + '/' + NODE_SCRIPT_NAME_SHELL;
        }
        catch( std::exception &e )
        {
            PS_LOG( "ShellExec::Init: " << e.what() );
            return false;
        }
        return true;
    }
};

class RubyExec : public ScriptExec
{
protected:
    virtual bool InitLanguageEnv()
    {
        try
        {
            exePath_ = common::Config::Instance().Get<string>( "ruby" );
            nodePath_ = exeDir + '/' + NODE_SCRIPT_NAME_RUBY;
        }
        catch( std::exception &e )
        {
            PS_LOG( "RubyExec::Init: " << e.what() );
            return false;
        }
        return true;
    }
};

class JavaScriptExec : public ScriptExec
{
protected:
    virtual bool InitLanguageEnv()
    {
        try
        {
            exePath_ = common::Config::Instance().Get<string>( "js" );
            nodePath_ = exeDir + '/' + NODE_SCRIPT_NAME_JS;
        }
        catch( std::exception &e )
        {
            PS_LOG( "JavaScriptExec::Init: " << e.what() );
            return false;
        }
        return true;
    }
};

class ExecCreator
{
public:
    virtual ScriptExec *Create( const std::string &language )
    {
        if ( language == "python" )
            return new PythonExec();
        if ( language == "java" )
            return new JavaExec();
        if ( language == "shell" )
            return new ShellExec();
        if ( language == "ruby" )
            return new RubyExec();
        if ( language == "js" )
            return new JavaScriptExec();
        return NULL;
    }
};


class Session
{
protected:
    template< typename T >
    void HandleRequest( T &request )
    {
        const std::string &requestStr = request.GetString();
        std::istringstream ss( requestStr );

        boost::property_tree::ptree ptree;
        boost::property_tree::read_json( ss, ptree );

        std::string task = ptree.get<std::string>( "task" );

        if ( task == "exec" )
        {
            JobExec job;
            job.ParseRequest( ptree );
            Execute( job );
        }
        else
        if ( task == "stop_task" )
        {
            JobStopTask job;
            job.ParseRequest( ptree );
            StopTaskAction action;
            action.StopTask( job );
            errCode_ = job.GetError();
        }
        else
        if ( task == "stop_prev" )
        {
            JobStopPreviousJobs job;
            job.ParseRequest( ptree );
            StopPreviousJobsAction action;
            action.StopJobs( job );
            errCode_ = job.GetError();
        }
        else
        {
            PS_LOG( "Session::HandleRequest: unknow task: " << task );
            errCode_ = NODE_FATAL;
        }
    }

    void GetResponse( std::string &response ) const
    {
        std::ostringstream ss;
        boost::property_tree::ptree ptree;

        ptree.put( "err", errCode_ );
        ptree.put( "elapsed", execTime_ );

        boost::property_tree::write_json( ss, ptree, false );
        size_t responseLength = ss.str().size();
        response = boost::lexical_cast< std::string >( responseLength );
        response += '\n';
        response += ss.str();
    }

private:
    void Execute( JobExec &job )
    {
        ExecCreator execCreator;
        boost::scoped_ptr< ScriptExec > scriptExec(
            execCreator.Create( job.GetScriptLanguage() )
        );
        if ( scriptExec )
        {
            scriptExec->Execute( &job );
            errCode_ = job.GetError();
            execTime_ = job.GetExecTime();
        }
        else
        {
            PS_LOG( "Session::HandleRequest: appropriate executor not found for language: "
                    << job.GetScriptLanguage() );
            errCode_ = NODE_LANG_NOT_SUPPORTED;
        }
    }

protected:
    int errCode_;
    int64_t execTime_;
};

class SessionBoost : public Session, public boost::enable_shared_from_this< SessionBoost >
{
    typedef boost::array< char, 2048 > BufferType;

public:
    SessionBoost( boost::asio::io_service &io_service )
    : socket_( io_service ), request_( true )
    {
    }

    virtual ~SessionBoost()
    {
        cout << "E: ~Session()" << endl;
    }

    void Start()
    {
        memset( buffer_.c_array(), 0, buffer_.size() );
        socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                 boost::bind( &SessionBoost::FirstRead, shared_from_this(),
                                              boost::asio::placeholders::error,
                                              boost::asio::placeholders::bytes_transferred ) );
    }

    tcp::socket &GetSocket()
    {
        return socket_;
    }

protected:
    void FirstRead( const boost::system::error_code& error, size_t bytes_transferred )
    {
        if ( !error )
        {
            int ret = request_.OnFirstRead( buffer_, bytes_transferred );
            if ( ret == 0 )
            {
                socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                         boost::bind( &SessionBoost::FirstRead, shared_from_this(),
                                                      boost::asio::placeholders::error,
                                                      boost::asio::placeholders::bytes_transferred ) );
                return;
            }
            if ( ret < 0 )
            {
                errCode_ = NODE_FATAL;
                WriteResponse();
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
                                         boost::bind( &SessionBoost::HandleRead, shared_from_this(),
                                                    boost::asio::placeholders::error,
                                                    boost::asio::placeholders::bytes_transferred ) );
            }
            else
            {
                HandleRequest( request_ );

                request_.Reset();
                Start();
                WriteResponse();
            }
        }
        else
        {
            PS_LOG( "SessionBoost::HandleRead error=" << error.message() );
            //HandleError( error );
        }
    }

    void WriteResponse()
    {
        GetResponse( response_ );

        boost::asio::async_write( socket_,
                                  boost::asio::buffer( response_ ),
                                  boost::bind( &SessionBoost::HandleWrite, shared_from_this(),
                                               boost::asio::placeholders::error,
                                               boost::asio::placeholders::bytes_transferred ) );
    }

    void HandleWrite( const boost::system::error_code& error, size_t bytes_transferred )
    {
        if ( error )
        {
            PS_LOG( "SessionBoost::HandleWrite error=" << error.message() );
        }
    }

protected:
    tcp::socket socket_;
    BufferType buffer_;
    common::Request< BufferType > request_;
    std::string response_;
};


class ConnectionAcceptor
{
    typedef boost::shared_ptr< SessionBoost > session_ptr;

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
            PS_LOG( "ConnectionAcceptor: " << e.what() );
        }

        StartAccept();
    }

private:
    void StartAccept()
    {
        session_ptr session( new SessionBoost( io_service_ ) );
        acceptor_.async_accept( session->GetSocket(),
                                boost::bind( &ConnectionAcceptor::HandleAccept, this,
                                             session, boost::asio::placeholders::error ) );
    }

    void HandleAccept( session_ptr session, const boost::system::error_code &error )
    {
        if ( !error )
        {
            cout << "connection accepted..." << endl;
            io_service_.post( boost::bind( &SessionBoost::Start, session ) );
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

} // namespace worker


namespace {

void SigHandler( int s )
{
    if ( s == SIGCHLD )
    {
        // On Linux, multiple children terminating will be compressed into a single SIGCHLD
        while( 1 )
        {
            int status;
            pid_t pid = waitpid( -1, &status, WNOHANG );
            if ( pid > 0 )
                worker::childProcesses.Delete( pid );
            else
                break;
        }
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

void SetupPrExecIPC()
{
    namespace ipc = boost::interprocess; 

    try
    {
        worker::sharedMemPool = new ipc::shared_memory_object( ipc::open_only, worker::SHMEM_NAME, ipc::read_only );
        worker::mappedRegion = new ipc::mapped_region( *worker::sharedMemPool, ipc::read_only );
    }
    catch( std::exception &e )
    {
        PS_LOG( "SetupPrExecIPC failed: " << e.what() );
        exit( 1 );
    }
}

void SetupLanguageRuntime()
{
    pid_t pid = fork();
    if ( pid == 0 )
    {
        worker::isFork = true;
        std::string javacPath;
        try
        {
            javacPath = common::Config::Instance().Get<std::string>( "javac" );
        }
        catch( std::exception &e )
        {
            PS_LOG( "SetupLanguageRuntime: get javac path failed: " << e.what() );
        }
        std::string nodePath = worker::exeDir + '/' + worker::NODE_SCRIPT_NAME_JAVA;
        if ( access( javacPath.c_str(), F_OK ) != -1 )
        {
            int ret = execl( javacPath.c_str(), "javac", nodePath.c_str(), NULL );
            if ( ret < 0 )
            {
                PS_LOG( "SetupLanguageRuntime: execl(javac) failed: " << strerror(errno) );
            }
        }
        else
        {
            PS_LOG( "SetupLanguageRuntime: file not found: " << javacPath );
        }
        ::exit( 1 );
    }
    else
    if ( pid > 0 )
    {
        int status;
        waitpid( pid, &status, 0 );
    }
    else
    if ( pid < 0 )
    {
        PS_LOG( "SetupLanguageRuntime: fork() failed " << strerror(errno) );
    }
}

void Impersonate()
{
    if ( worker::uid )
    {
        int ret = setuid( worker::uid );
        if ( ret < 0 )
        {
            PS_LOG( "impersonate uid=" << worker::uid << " failed : " << strerror(errno) );
            exit( 1 );
        }

        PS_LOG( "successfully impersonated, uid=" << worker::uid );
    }
}

void CleanupThreads()
{
    worker::ThreadInfo::iterator it;
    for( it = worker::threadInfo.begin();
         it != worker::threadInfo.end();
       ++it )
    {
        worker::ThreadParams &threadParams = it->second;

        if ( threadParams.readFifoFD != -1 )
        {
            close( threadParams.readFifoFD );
            threadParams.readFifoFD = -1;
        }

        if ( !threadParams.readFifo.empty() )
        {
            unlink( threadParams.readFifo.c_str() );
            threadParams.readFifo.clear();
        }

        if ( threadParams.writeFifoFD != -1 )
        {
            close( threadParams.writeFifoFD );
            threadParams.writeFifoFD = -1;
        }

        if ( !threadParams.writeFifo.empty() )
        {
            unlink( threadParams.writeFifo.c_str() );
            threadParams.writeFifo.clear();
        }
    }
}

void AtExit()
{
    if ( worker::isFork )
        return;

    CleanupThreads();

    delete worker::mappedRegion;
    worker::mappedRegion = NULL;

    delete worker::sharedMemPool;
    worker::sharedMemPool = NULL;

    common::logger::ShutdownLogger();

    kill( getppid(), SIGTERM );
}

int CreateFifo( const std::string &fifoName )
{
    unlink( fifoName.c_str() );

    int ret = mkfifo( fifoName.c_str(), S_IRUSR | S_IWUSR );
    if ( !ret )
    {
        if ( worker::uid )
        {
            ret = chown( fifoName.c_str(), worker::uid, (gid_t)-1 );
            if ( ret == -1 )
                PS_LOG( "CreateFifo: chown failed " << strerror(errno) );
        }

        int fifofd = open( fifoName.c_str(), O_RDWR | O_NONBLOCK );
        if ( fifofd == -1 )
        {
            PS_LOG( "open fifo " << fifoName << " failed: " << strerror(errno) );
        }
        return fifofd;
    }
    else
    {
        PS_LOG( "CreateFifo: mkfifo failed " << strerror(errno) );
    }
    return -1;
}

void OnThreadCreate( const boost::thread *thread )
{
    static int threadCnt = 0;

    worker::ThreadParams threadParams;
    threadParams.writeFifoFD = -1;
    threadParams.readFifoFD = -1;

    std::ostringstream ss;
    ss << worker::FIFO_NAME << 'w' << threadCnt;
    threadParams.writeFifo = ss.str();

    threadParams.writeFifoFD = CreateFifo( threadParams.writeFifo );
    if ( threadParams.writeFifoFD == -1 )
    {
        threadParams.writeFifo.clear();
    }

    std::ostringstream ss2;
    ss2 << worker::FIFO_NAME << 'r' << threadCnt;
    threadParams.readFifo = ss2.str();

    threadParams.readFifoFD = CreateFifo( threadParams.readFifo );
    if ( threadParams.readFifoFD == -1 )
    {
        threadParams.readFifo.clear();
    }

    worker::threadInfo[ thread->get_id() ] = threadParams;
    ++threadCnt;
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
    SetupSignalHandlers();
    SetupSignalMask();
    atexit( AtExit );

    try
    {
        // initialization
        worker::isDaemon = false;
        worker::isFork = false;
        worker::uid = 0;

        // parse input command line options
        namespace po = boost::program_options;
        
        po::options_description descr;

        descr.add_options()
            ("num_thread", po::value<unsigned int>(), "Thread pool size")
            ("exe_dir", po::value<std::string>(), "Executable working directory")
            ("d", "Run as a daemon")
            ("u", po::value<uid_t>(), "Start as a specific non-root user")
            ("f", "Create process for each request");
        
        po::variables_map vm;
        po::store( po::parse_command_line( argc, argv, descr ), vm );
        po::notify( vm );

        if ( vm.count( "d" ) )
        {
            worker::isDaemon = true;
        }

        common::logger::InitLogger( worker::isDaemon, "prexec" );

        if ( vm.count( "u" ) )
        {
            worker::uid = vm[ "u" ].as<uid_t>();
        }

        if ( vm.count( "num_thread" ) )
        {
            worker::numThread = vm[ "num_thread" ].as<unsigned int>();
        }

        if ( vm.count( "exe_dir" ) )
        {
            worker::exeDir = vm[ "exe_dir" ].as<std::string>();
        }

        common::Config::Instance().ParseConfig( worker::exeDir.c_str() );

        SetupLanguageRuntime();

        SetupPrExecIPC();
        
        // start accepting connections
        boost::asio::io_service io_service;

        worker::ConnectionAcceptor acceptor( io_service, worker::DEFAULT_PREXEC_PORT );

        // create thread pool
        boost::thread_group worker_threads;
        for( unsigned int i = 0; i < worker::numThread; ++i )
        {
            boost::thread *thread = worker_threads.create_thread(
                boost::bind( &ThreadFun, &io_service )
            );
            OnThreadCreate( thread );
        }

        // signal parent process to say that PrExec has been initialized
        kill( getppid(), SIGUSR1 );

        Impersonate();

        UnblockSighandlerMask();

        if ( worker::isDaemon )
        {
			PS_LOG( "started" );
        }

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

        io_service.stop();

        worker::execTable.Clear();

        CleanupThreads();

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

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
#include <mutex>
#include <condition_variable>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/array.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <unistd.h>
#include <csignal>
#include <sys/wait.h>
#include "common.h"
#include "common/security.h"
#include "common/request.h"
#include "common/log.h"
#include "common/config.h"
#include "common/error_code.h"
#include "common/configure.h"
#include "exec_info.h"
#include "exec_job.h"
#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif
#ifdef HAVE_EXEC_INFO_H
#include "common/stack.h"
#endif

using namespace std;
using boost::asio::local::stream_protocol;

namespace worker {

bool g_isFork;
int g_processCompletionPipe;


struct ThreadParams
{
    int writeFifoFD, readFifoFD;
    string writeFifo, readFifo;
    ExecInfo execInfo;
};

typedef std::map< std::thread::id, ThreadParams > ThreadInfo;

class ExecContext
{
private:
    void SetExeDir( const std::string &dir ) { exeDir_ = dir; }
    void SetResourcesDir( const std::string &dir ) { resourcesDir_ = dir; }

    void SetMappedRegion( std::shared_ptr< boost::interprocess::mapped_region > &mappedRegion )
    {
        mappedRegion_ = mappedRegion;
    }

public:
    ExecTable &GetExecTable() { return execTable_; }
    ThreadInfo &GetThreadInfo() { return threadInfo_; }

    const std::string &GetExeDir() const { return exeDir_; }
    const std::string &GetResourcesDir() const { return resourcesDir_; }

    const std::shared_ptr< boost::interprocess::mapped_region > &GetMappedRegion() const
    {
        return mappedRegion_;
    }

    PidContainer &GetChildProcesses() { return childProcesses_; }

    std::mutex &GetProcessCompletionMutex() { return processCompletionMutex_; }
    std::condition_variable &GetProcessCompletionCondition() { return processCompletionCondition_; }

private:
    ExecTable execTable_;
    ThreadInfo threadInfo_;

    std::string exeDir_;
    std::string resourcesDir_;

    std::shared_ptr< boost::interprocess::mapped_region > mappedRegion_;

    PidContainer childProcesses_;

    std::mutex processCompletionMutex_;
    std::condition_variable processCompletionCondition_;

    friend class ExecApplication;
};

typedef std::shared_ptr< ExecContext > ExecContextPtr;


void FlushFifo( int fifo )
{
    pollfd pfd[1];
    pfd[0].fd = fifo;
    pfd[0].events = POLLIN;
    char buf[1024];
    while( 1 )
    {
        int ret = poll( pfd, 1, 0 );
        if ( ret > 0 )
        {
            ret = read( fifo, buf, sizeof( buf ) );
            if ( ret > 0 )
                continue;
            else
            if ( ret < 0 )
            {
                PLOG_ERR( "FlushFifo: read failed: fd=" << fifo << ", err=" << strerror(errno) );
            }
        }
        else
        if ( ret < 0 )
        {
            PLOG_ERR( "FlushFifo: poll failed: fd=" << pfd << ", err=" << strerror(errno) );
        }
        break;
    }
}

class StopTaskAction
{
public:
    StopTaskAction( ExecContextPtr &execContext )
    : execContext_( execContext )
    {}

    void StopTask( JobStopTask &job )
    {
        PLOG_DBG( "StopTaskAction::StopTask: jobId=" << job.GetJobId() << ", taskId=" << job.GetTaskId() );

        ExecTable &execTable = execContext_->GetExecTable();

        ExecInfo execInfo;
        if ( execTable.Find( job.GetJobId(), job.GetTaskId(), job.GetMasterId(), execInfo ) &&
             execTable.Delete( job.GetJobId(), job.GetTaskId(), job.GetMasterId() ) )
        {
            pid_t pid = execInfo.pid_;

            if ( execContext_->GetChildProcesses().Find( pid ) )
            {
                int ret = kill( -pid, SIGTERM );
                if ( ret != -1 )
                {
                    PLOG( "StopTaskAction::StopTask: task stopped, pid=" << pid <<
                          ", jobId=" << job.GetJobId() << ", taskId=" << job.GetTaskId() );
                }
                else
                {
                    PLOG( "StopTaskAction::StopTask: process group killing failed: pgid=" << pid << ", err=" << strerror(errno) );
                    job.OnError( NODE_FATAL );
                }
            }
            else
            {
                PLOG( "StopTaskAction::StopTask: child process already terminated" );
                job.OnError( NODE_TASK_NOT_FOUND );
            }

            int readFifo, writeFifo;
            if ( FindFifo( execInfo, readFifo, writeFifo ) )
            {
                // Stop ScriptExec::ReadCompletionStatus read waiting
                std::string errCode = std::to_string( NODE_JOB_TIMEOUT );
                int ret = write( readFifo, errCode.c_str(), errCode.size() );
                if ( ret == -1 )
                    PLOG( "StopTaskAction::StopTask: write fifo failed, err=" << strerror(errno) );

                // ScriptExec::WriteScript may be uncompleted while someone else killing exec task,
                // so we must emulate fifo reading from the opposite side (what exec task should do)
                worker::FlushFifo( writeFifo );
            }
            else
            {
                PLOG( "StopTaskAction::StopTask: fifo not found for pid=" << pid );
            }
        }
        else
        {
            PLOG( "StopTaskAction::StopTask: task not found, jobId=" << job.GetJobId() <<
                  ", taskId=" << job.GetTaskId() << ", " << "masterId=" << job.GetMasterId() );
            job.OnError( NODE_TASK_NOT_FOUND );
        }
    }

private:
    bool FindFifo( const ExecInfo &execInfo, int &readFifo, int &writeFifo ) const
    {
        ThreadInfo &threadInfo = execContext_->GetThreadInfo();

        for( const auto &it : threadInfo )
        {
            const ThreadParams &threadParams = it.second;
            if ( threadParams.execInfo == execInfo )
            {
                readFifo = threadParams.readFifoFD;
                writeFifo = threadParams.writeFifoFD;
                return true;
            }
        }
        return false;
    }

private:
    ExecContextPtr &execContext_;
};

class StopPreviousJobsAction
{
public:
    StopPreviousJobsAction( ExecContextPtr &execContext )
    : execContext_( execContext )
    {}

    void StopJobs( JobStopPreviousJobs &job )
    {
        PLOG_DBG( "StopPreviousJobsAction::StopJobs" );

        ExecTable &execTable = execContext_->GetExecTable();

        std::list< ExecInfo > table;
        execTable.Get( table );
        for( const auto &execInfo : table )
        {
            if ( execInfo.masterId_ != job.GetMasterId() )
            {
                JobStopTask job( execInfo.jobId_, execInfo.taskId_, execInfo.masterId_ );
                StopTaskAction action( execContext_ );
                action.StopTask( job );
            }
        }
    }

private:
    ExecContextPtr &execContext_;
};

class StopAllJobsAction
{
public:
    StopAllJobsAction( ExecContextPtr &execContext )
    : execContext_( execContext )
    {}

    void StopJobs()
    {
        PLOG_DBG( "StopAllJobsAction::StopJobs" );

        ExecTable &execTable = execContext_->GetExecTable();
        execTable.Clear();
    }

private:
    ExecContextPtr &execContext_;
};

class ScriptExec
{
public:
    virtual ~ScriptExec() {}

    void SetExecContext( ExecContextPtr &execContext )
    {
        execContext_ = execContext;
    }

    virtual void Execute( JobExec *job )
    {
        PLOG_DBG( "ScriptExec::Execute" );

        if ( !InitLanguageEnv() )
        {
            job->OnError( NODE_FATAL );
            return;
        }

        job_ = job;

        std::string job_file_dir;
        if ( job_->IsFromFile() )
        {
            const std::string &scriptPath = job_->GetFilePath();
            boost::filesystem::path p( scriptPath );
            job_file_dir = p.parent_path().string();
            auto job_file_size = boost::filesystem::file_size( p );
            job_->SetScriptLength( job_file_size );
        }

        auto scriptLength = std::to_string( job->GetScriptLength() );

        auto taskId = std::to_string( job->GetTaskId() );
        auto numTasks = std::to_string( job->GetNumTasks() );
        auto jobId = std::to_string( job->GetJobId() );

        ThreadInfo &threadInfo = execContext_->GetThreadInfo();
        const ThreadParams &threadParams = threadInfo[ std::this_thread::get_id() ];

        // NB: avoid unsafe calls between fork() and exec() (e.g. malloc)
        pid_t pid = DoFork( job_file_dir );
        if ( pid != 0 )
            return;

        DoExec( threadParams, scriptLength, taskId, numTasks, jobId );
    }

    virtual void KillExec( pid_t pid )
    {
        PLOG( "poll timed out, trying to kill process: " << pid );

        ExecTable &execTable = execContext_->GetExecTable();
        if ( execTable.Delete( job_->GetJobId(), job_->GetTaskId(), job_->GetMasterId() ) &&
             execContext_->GetChildProcesses().Find( pid ) )
        {
            int ret = kill( -pid, SIGTERM );
            if ( ret == -1 )
            {
                PLOG( "process group killing failed: pgid=" << pid << ", err=" << strerror(errno) );
            }
        }
        else
        {
            PLOG( "ScriptExec::KillExec: it seems that process is already killed, pid=" << pid );
        }
    }

    static void Cancel( const ExecInfo &execInfo, ExecContextPtr &execContext )
    {
        JobStopTask job( execInfo.jobId_, execInfo.taskId_, execInfo.masterId_ );
        StopTaskAction action( execContext );
        action.StopTask( job );
    }

protected:
    virtual bool InitLanguageEnv() = 0;

    pid_t DoFork( const std::string &job_file_dir )
    {
        // flush fifo before fork, because after forking completion status of a
        // forked process may be lost
        FlushFifo();

        struct timeval tvStart, tvEnd;
        gettimeofday( &tvStart, nullptr );

        pid_t pid = fork();

        if ( pid > 0 )
        {
            PLOG( "ScriptExec::DoFork: started process pid=" << pid );

            execContext_->GetChildProcesses().Add( pid );

            ThreadInfo &threadInfo = execContext_->GetThreadInfo();
            ThreadParams &threadParams = threadInfo[ std::this_thread::get_id() ];

            ExecTable &execTable = execContext_->GetExecTable();

            ExecInfo execInfo;
            execInfo.jobId_ = job_->GetJobId();
            execInfo.taskId_ = job_->GetTaskId();
            execInfo.masterId_ = job_->GetMasterId();
            execInfo.pid_ = pid;

            threadParams.execInfo = execInfo; // copy without callback

            execInfo.callback_ = std::bind( &ScriptExec::Cancel, execInfo, execContext_ );
            execTable.Add( execInfo );

            bool succeded = WriteScript( threadParams.writeFifoFD );
            if ( succeded )
            {
                succeded = ReadCompletionStatus( threadParams.readFifoFD, pid );
            }

            gettimeofday( &tvEnd, nullptr );
            const int64_t elapsed = static_cast<int64_t>( ( tvEnd.tv_sec - tvStart.tv_sec ) * 1000 +
                                                          ( tvEnd.tv_usec - tvStart.tv_usec ) / 1000 );
            job_->SetExecTime( elapsed );

            if ( succeded )
            {
                PLOG( "ScriptExec::DoFork: process returned error code: pid=" << pid <<
                      ", jobId=" << job_->GetJobId() << ", taskId=" << job_->GetTaskId() << ", err=" << job_->GetError() );
            }
            else
            {
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
            pthread_sigmask( SIG_UNBLOCK, &sigset, nullptr );

            g_isFork = true;

            if ( setpgid( getpid(), getpid() ) < 0 )
            {
                PLOG_ERR( "ScriptExec::DoFork: setpgid() failed: " << strerror(errno) );
            }

            // linux-only. kill child process, if parent exits
#ifdef HAVE_SYS_PRCTL_H
            prctl( PR_SET_PDEATHSIG, SIGHUP );
#endif

            if ( !job_file_dir.empty() )
            {
                int ret = chdir( job_file_dir.c_str() );
                if ( ret < 0 )
                {
                    PLOG_WRN( "ScriptExec::DoFork: chdir failed: dir='" << job_file_dir << "', err=" << strerror(errno) );
                }
            }
        }
        else
        {
            PLOG_ERR( "ScriptExec::DoFork: fork() failed " << strerror(errno) );
            job_->OnError( NODE_FATAL );
        }

        return pid;
    }

    virtual void DoExec( const ThreadParams &threadParams, const std::string &scriptLength,
                         const std::string &taskId, const std::string &numTasks, const std::string &jobId )
    {
        int ret = execl( exePath_.c_str(), job_->GetScriptLanguage().c_str(),
                         nodePath_.c_str(),
                         threadParams.readFifo.c_str(), threadParams.writeFifo.c_str(),
                         scriptLength.c_str(),
                         taskId.c_str(), numTasks.c_str(), jobId.c_str(), nullptr );
        if ( ret < 0 )
        {
            PLOG_ERR( "ScriptExec::DoExec: execl failed: " << strerror(errno) );
        }
        ::exit( 1 );
    }

    bool WriteScript( int fifo )
    {
        std::string scriptData;
        const char *scriptAddr = nullptr;
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
                PLOG_ERR( "ScriptExec::WriteScript: couldn't open " << filePath );
                job_->OnError( NODE_SCRIPT_FILE_NOT_FOUND );
                return false;
            }
            scriptAddr = scriptData.c_str();
            bytesToWrite = scriptData.size();
        }
        else
        {
            // read script from shared memory
            size_t offset = job_->GetCommId() * SHMEM_BLOCK_SIZE;
            const std::shared_ptr< boost::interprocess::mapped_region > &mappedRegion(
                execContext_->GetMappedRegion()
            );
            scriptAddr = static_cast< const char* >( mappedRegion->get_address() ) + offset;
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
                    PLOG_WRN( "ScriptExec::WriteScript: write failed: fd=" << fifo << ", err=" << strerror(errno) );
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
            PLOG_WRN( "ScriptExec::WriteScript: poll failed: fd=" << pfd << ", err=" << strerror(errno) );
        }

        job_->OnError( errCode );
        return false;
    }

    bool ReadCompletionStatus( int fifo, pid_t pid )
    {
        int64_t timeout = job_->GetTimeout();
        if ( timeout > 0 )
        {
            struct timeval tvStart, tvEnd;
            timeout *= 1000;

            std::unique_lock< std::mutex > lock( execContext_->GetProcessCompletionMutex() );
            while( execContext_->GetChildProcesses().Find( pid ) )
            {
                gettimeofday( &tvStart, nullptr );
                execContext_->GetProcessCompletionCondition().wait_for( lock, std::chrono::milliseconds( timeout ) );
                gettimeofday( &tvEnd, nullptr );

                const int64_t elapsed = static_cast<int64_t>( ( tvEnd.tv_sec - tvStart.tv_sec ) * 1000 +
                                                              ( tvEnd.tv_usec - tvStart.tv_usec ) / 1000 );
                timeout -= elapsed;
                if ( timeout <= 0 )
                    break; // deadline reached
            }
        }
        else
        {
            std::unique_lock< std::mutex > lock( execContext_->GetProcessCompletionMutex() );
            while( execContext_->GetChildProcesses().Find( pid ) )
            {
                execContext_->GetProcessCompletionCondition().wait_for( lock, std::chrono::seconds( 1 ) );
            }
        }

        int errCode = NODE_FATAL;

        pollfd pfd[1];
        pfd[0].fd = fifo;
        pfd[0].events = POLLIN;

        int ret = poll( pfd, 1, 0 );
        if ( ret > 0 )
        {
            char buf[32];
            memset( buf, 0, sizeof( buf ) );
            ret = read( fifo, &buf, sizeof( buf ) );
            if ( ret > 0 )
            {
                try
                {
                    errCode = boost::lexical_cast<int>( buf );
                }
                catch( std::exception &e )
                {
                    PLOG_WRN( "ScriptExec::ReadCompletionStatus: " << e.what() );
                }
                job_->OnError( errCode );
                return true;
            }
            else
            {
                PLOG_WRN( "ScriptExec::ReadCompletionStatus: read fifo failed: fd=" << fifo << ", err=" << strerror(errno) );
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
            PLOG_WRN( "ScriptExec::ReadCompletionStatus: poll failed: fd=" << pfd << ", err=" << strerror(errno) );
        }

        job_->OnError( errCode );
        return false;
    }

private:
    void FlushFifo()
    {
        ThreadInfo &threadInfo = execContext_->GetThreadInfo();
        const ThreadParams &threadParams = threadInfo[ std::this_thread::get_id() ];
        worker::FlushFifo( threadParams.readFifoFD );
        worker::FlushFifo( threadParams.writeFifoFD );
    }

protected:
    JobExec *job_;
    std::string exePath_;
    std::string nodePath_;
    ExecContextPtr execContext_;
};

class PythonExec : public ScriptExec
{
protected:
    virtual bool InitLanguageEnv()
    {
        try
        {
            exePath_ = common::Config::Instance().Get<string>( "python" );
            if ( execContext_->GetResourcesDir().empty() )
            {
                nodePath_ = execContext_->GetExeDir();
            }
            else
            {
                nodePath_ = execContext_->GetResourcesDir();
            }
            nodePath_ += std::string( "/" ) + NODE_SCRIPT_NAME_PY;
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "PythonExec::Init: " << e.what() );
            return false;
        }
        return true;
    }
};

class JavaExec : public ScriptExec
{
protected:
    virtual void DoExec( const ThreadParams &threadParams, const std::string &scriptLength,
                         const std::string &taskId, const std::string &numTasks, const std::string &jobId )
    {
        int ret = execl( exePath_.c_str(), job_->GetScriptLanguage().c_str(),
                         "-cp", nodePath_.c_str(),
                         "node",
                         threadParams.readFifo.c_str(), threadParams.writeFifo.c_str(),
                         scriptLength.c_str(),
                         taskId.c_str(), numTasks.c_str(), jobId.c_str(), nullptr );
        if ( ret < 0 )
        {
            PLOG_ERR( "JavaExec::DoExec: execl failed: " << strerror(errno) );
        }
        ::exit( 1 );
    }

    virtual bool InitLanguageEnv()
    {
        try
        {
            exePath_ = common::Config::Instance().Get<string>( "java" );
            if ( execContext_->GetResourcesDir().empty() )
            {
                nodePath_ = execContext_->GetExeDir();
            }
            else
            {
                nodePath_ = execContext_->GetResourcesDir();
            }
            nodePath_ += "/node";
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "JavaExec::Init: " << e.what() );
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
            if ( execContext_->GetResourcesDir().empty() )
            {
                nodePath_ = execContext_->GetExeDir();
            }
            else
            {
                nodePath_ = execContext_->GetResourcesDir();
            }
            nodePath_ += std::string( "/" ) + NODE_SCRIPT_NAME_SHELL;
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "ShellExec::Init: " << e.what() );
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
            if ( execContext_->GetResourcesDir().empty() )
            {
                nodePath_ = execContext_->GetExeDir();
            }
            else
            {
                nodePath_ = execContext_->GetResourcesDir();
            }
            nodePath_ += std::string( "/" ) + NODE_SCRIPT_NAME_RUBY;
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "RubyExec::Init: " << e.what() );
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
            if ( execContext_->GetResourcesDir().empty() )
            {
                nodePath_ = execContext_->GetExeDir();
            }
            else
            {
                nodePath_ = execContext_->GetResourcesDir();
            }
            nodePath_ += std::string( "/" ) + NODE_SCRIPT_NAME_JS;
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "JavaScriptExec::Init: " << e.what() );
            return false;
        }
        return true;
    }
};

class ExecCreator
{
public:
    ScriptExec *Create( const std::string &language )
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
        return nullptr;
    }
};


class Session
{
protected:
    Session( ExecContextPtr &execContext )
    : execContext_( execContext )
    {}

    virtual ~Session()
    {
        PLOG_DBG( "destroying session" );
    }

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
            StopTaskAction action( execContext_ );
            action.StopTask( job );
            errCode_ = job.GetError();
        }
        else
        if ( task == "stop_prev" )
        {
            JobStopPreviousJobs job;
            job.ParseRequest( ptree );
            StopPreviousJobsAction action( execContext_ );
            action.StopJobs( job );
            errCode_ = job.GetError();
        }
        else
        if ( task == "stop_all" )
        {
            StopAllJobsAction action( execContext_ );
            action.StopJobs();
            errCode_ = 0;
        }
        else
        {
            PLOG_WRN( "Session::HandleRequest: unknow task: " << task );
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
        response = std::to_string( responseLength );
        response += '\n';
        response += ss.str();
    }

private:
    void Execute( JobExec &job )
    {
        ExecCreator execCreator;
        std::unique_ptr< ScriptExec > scriptExec(
            execCreator.Create( job.GetScriptLanguage() )
        );
        if ( scriptExec )
        {
            scriptExec->SetExecContext( execContext_ );
            scriptExec->Execute( &job );
            errCode_ = job.GetError();
            execTime_ = job.GetExecTime();
        }
        else
        {
            PLOG_WRN( "Session::HandleRequest: appropriate executor not found for language: "
                      << job.GetScriptLanguage() );
            errCode_ = NODE_LANG_NOT_SUPPORTED;
        }
    }

protected:
    int errCode_;
    int64_t execTime_;
    ExecContextPtr execContext_;
};

class SessionBoost : public Session, public boost::enable_shared_from_this< SessionBoost >
{
    typedef boost::array< char, 2048 > BufferType;

public:
    SessionBoost( boost::asio::io_service &io_service,
                  ExecContextPtr &execContext )
    : Session( execContext ),
    socket_( io_service ), request_( true )
    {}

    void Start()
    {
        buffer_.fill( 0 );
        socket_.async_read_some( boost::asio::buffer( buffer_ ),
                                 boost::bind( &SessionBoost::FirstRead, shared_from_this(),
                                              boost::asio::placeholders::error,
                                              boost::asio::placeholders::bytes_transferred ) );
    }

    stream_protocol::socket &GetSocket()
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
            PLOG_WRN( "SessionBoost::HandleRead error=" << error.message() );
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
            PLOG_ERR( "SessionBoost::HandleWrite error=" << error.message() );
        }
    }

protected:
    stream_protocol::socket socket_;
    BufferType buffer_;
    common::Request< BufferType > request_;
    std::string response_;
};


class ConnectionAcceptor
{
    typedef boost::shared_ptr< SessionBoost > session_ptr;
    typedef std::shared_ptr< stream_protocol::acceptor > acceptor_ptr;

public:
    ConnectionAcceptor( boost::asio::io_service &io_service,
                        uid_t uid,
                        ExecContextPtr &execContext )
    : io_service_( io_service ),
     execContext_( execContext )
    {
        try
        {
            int ret = unlink( UDS_NAME );
            if ( ret < 0 && errno != ENOENT )
            {
                PLOG_ERR( "ConnectionAcceptor::ConnectionAcceptor: unlink failed: file=" << UDS_NAME << ", err=" << strerror(errno) );
            }
            stream_protocol::endpoint endpoint( UDS_NAME );
            acceptor_ = make_shared<stream_protocol::acceptor>( io_service, endpoint );

            if ( uid )
            {
                int ret = chown( UDS_NAME, uid, static_cast<gid_t>( -1 ) );
                if ( ret == -1 )
                    PLOG_ERR( "ConnectionAcceptor: chown failed: file=" << UDS_NAME << ", uid=" << uid << ", err=" << strerror(errno) );
            }
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "ConnectionAcceptor: " << e.what() );
            ::exit( 1 );
        }

        StartAccept();
    }

private:
    void StartAccept()
    {
        session_ptr session( new SessionBoost( io_service_, execContext_ ) );
        acceptor_->async_accept( session->GetSocket(),
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
            PLOG_ERR( "HandleAccept: " << error.message() );
        }
    }

private:
    boost::asio::io_service &io_service_;
    acceptor_ptr acceptor_;
    ExecContextPtr execContext_;
};

} // namespace worker


namespace {

void SigHandler( int s )
{
    switch( s )
    {
        case SIGCHLD:
        {
            // On Linux, multiple children terminating will be compressed into a single SIGCHLD
            while( 1 )
            {
                int status;
                pid_t pid = waitpid( -1, &status, WNOHANG );
                if ( pid > 0 )
                {
                    int err = write( worker::g_processCompletionPipe, &pid, sizeof( pid ) );
                    if ( err < 0 )
                    {
                        PLOG_WRN( "Signal " << strsignal( s ) << ", write() failed: " << errno );
                    }
                }
                else
                    break;
            }
            break;
        }

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

void ReadProcessCompletionPIDs( const worker::ExecContextPtr &spExecContext, int processCompletionPipe )
{
    pid_t pid;
    while( 1 )
    {
        int ret = read( processCompletionPipe, &pid, sizeof( pid ) );
        if ( ret > 0 )
        {
            PLOG( "ReadProcessCompletionPIDs: process terminated: pid=" << pid );

            spExecContext->GetChildProcesses().Delete( pid );
            std::unique_lock< std::mutex > lock( spExecContext->GetProcessCompletionMutex() );
            spExecContext->GetProcessCompletionCondition().notify_all();
        }
        else
        if ( ret == -1 )
        {
            PLOG_WRN( "ReadProcessCompletionPIDs: read failed: fd=" << processCompletionPipe << ", err=" << strerror(errno) );
            break;
        }
    }
}

void SetupLanguageRuntime( const worker::ExecContextPtr &execContext )
{
    pid_t pid = fork();
    if ( pid == 0 )
    {
        worker::g_isFork = true;
        std::string javacPath;
        try
        {
            javacPath = common::Config::Instance().Get<std::string>( "javac" );
        }
        catch( std::exception &e )
        {
            PLOG_WRN( "SetupLanguageRuntime: get javac path failed: " << e.what() );
        }
        std::string nodePath;
        if ( execContext->GetResourcesDir().empty() )
        {
            nodePath = execContext->GetExeDir();
        }
        else
        {
            nodePath = execContext->GetResourcesDir();
        }
        nodePath += std::string( "/" ) + worker::NODE_SCRIPT_NAME_JAVA;
        if ( access( javacPath.c_str(), F_OK ) != -1 )
        {
            int ret = execl( javacPath.c_str(), "javac", nodePath.c_str(), nullptr );
            if ( ret < 0 )
            {
                PLOG_WRN( "SetupLanguageRuntime: execl(javac) failed: " << strerror(errno) );
            }
        }
        else
        {
            PLOG_WRN( "SetupLanguageRuntime: file not found: " << javacPath );
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
        PLOG_ERR( "SetupLanguageRuntime: fork() failed " << strerror(errno) );
    }
}


void AtExit()
{
    if ( worker::g_isFork )
        return;

    common::logger::ShutdownLogger();

    kill( getppid(), SIGTERM );
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

class ExecApplication
{
public:
    ExecApplication( const std::string &exeDir, const std::string &resourcesDir,
                     const std::string &cfgDir, bool isDaemon, uid_t uid, unsigned int numThread )
    : cfgDir_( cfgDir ),
     isDaemon_( isDaemon ),
     uid_( uid ),
     numThread_( numThread ),
     processCompletionPipe_( -1 ),
     execContext_( make_shared<ExecContext>() )
    {
        execContext_->SetExeDir( exeDir );
        execContext_->SetResourcesDir( resourcesDir );
        g_processCompletionPipe = -1;
    }

    void Initialize()
    {
        common::Config &cfg = common::Config::Instance();
        if ( cfgDir_.empty() )
        {
            cfg.ParseConfig( execContext_->GetExeDir().c_str(), "worker.cfg" );
        }
        else
        {
            cfg.ParseConfig( "", cfgDir_.c_str() );
        }

        std::string logLevel = cfg.Get<std::string>( "log_level" );
        common::logger::InitLogger( isDaemon_, "prexec", logLevel.c_str() );

        PLOG_DBG( "ExecApplication::Initialize" );

        SetupLanguageRuntime( execContext_ );

        SetupPrExecIPC();

        InitProcessCompletionPipe();

        // start accepting connections
        acceptor_ = make_shared<ConnectionAcceptor>( io_service_, uid_, execContext_ );

        // create thread pool

        for( unsigned int i = 0; i < numThread_; ++i )
        {
            worker_threads_.emplace_back( ThreadFun, &io_service_ );
            OnThreadCreate( &worker_threads_.back() );
        }

        processCompletionThread_ = std::thread( ReadProcessCompletionPIDs, execContext_, processCompletionPipe_ );

        // signal parent process to say that PrExec has been initialized
        kill( getppid(), SIGUSR1 );

        common::ImpersonateOrExit( uid_ );

        const std::string &resourcesDir = execContext_->GetResourcesDir();
        if ( !resourcesDir.empty() && ( chdir( resourcesDir.c_str() ) < 0 ) )
        {
            PLOG_WRN( "ExecApplication::Initialize: chdir failed: dir='" << resourcesDir <<"', err=" << strerror(errno) );
        }
    }

    void Shutdown()
    {
        PLOG_DBG( "ExecApplication::Shutdown" );

        io_service_.stop();

        ExecTable &execTable = execContext_->GetExecTable();
        execTable.Clear();

        CleanupThreads();

        for( auto &t : worker_threads_ )
            t.join();

        CloseProcessCompletionPipe();
        processCompletionThread_.join();
    }

    void Run()
    {
        PLOG_DBG( "ExecApplication::Run" );

        UnblockSighandlerMask();

        if ( isDaemon_ )
        {
            PLOG( "started" );
        }

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
            PLOG_ERR( "ExecApplication::Run: sigwait failed: " << strerror(ret) );
        }
    }

private:
    int CreateFifo( const std::string &fifoName )
    {
        int ret = unlink( fifoName.c_str() );
        if ( ret < 0 && errno != ENOENT )
        {
            PLOG_ERR( "ExecApplication::CreateFifo: unlink failed: file=" << fifoName << ", err=" << strerror(errno) );
        }

        ret = mkfifo( fifoName.c_str(), S_IRUSR | S_IWUSR );
        if ( !ret )
        {
            if ( uid_ )
            {
                ret = chown( fifoName.c_str(), uid_, static_cast<gid_t>( -1 ) );
                if ( ret == -1 )
                    PLOG_ERR( "ExecApplication::CreateFifo: chown failed: file=" << fifoName << ", uid=" << uid_ << ", err=" << strerror(errno) );
            }

            int fifofd = open( fifoName.c_str(), O_RDWR | O_NONBLOCK );
            if ( fifofd == -1 )
            {
                PLOG_ERR( "ExecApplication::CreateFifo: open fifo " << fifoName << " failed: " << strerror(errno) );
            }
            return fifofd;
        }
        else
        {
            PLOG_ERR( "ExecApplication::CreateFifo: mkfifo failed: file=" << fifoName << ", err=" << strerror(errno) );
        }
        return -1;
    }

    void OnThreadCreate( const std::thread *thread )
    {
        static int threadCnt = 0;

        ThreadParams threadParams;
        threadParams.writeFifoFD = -1;
        threadParams.readFifoFD = -1;

        std::ostringstream ss;
        ss << FIFO_NAME << 'w' << threadCnt;
        threadParams.writeFifo = ss.str();

        threadParams.writeFifoFD = CreateFifo( threadParams.writeFifo );
        if ( threadParams.writeFifoFD == -1 )
        {
            threadParams.writeFifo.clear();
        }

        std::ostringstream ss2;
        ss2 << FIFO_NAME << 'r' << threadCnt;
        threadParams.readFifo = ss2.str();

        threadParams.readFifoFD = CreateFifo( threadParams.readFifo );
        if ( threadParams.readFifoFD == -1 )
        {
            threadParams.readFifo.clear();
        }

        ThreadInfo &threadInfo = execContext_->GetThreadInfo();
        threadInfo[ thread->get_id() ] = threadParams;
        ++threadCnt;
    }

    void CleanupThreads()
    {
        ThreadInfo &threadInfo = execContext_->GetThreadInfo();

        for( auto &it : threadInfo )
        {
            ThreadParams &threadParams = it.second;

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

        unlink( UDS_NAME );
    }

    void SetupPrExecIPC()
    {
        namespace ipc = boost::interprocess;

        try
        {
            sharedMemPool_ = make_shared<ipc::shared_memory_object>( ipc::open_only, SHMEM_NAME, ipc::read_only );
            mappedRegion_ = make_shared<ipc::mapped_region>( *sharedMemPool_.get(), ipc::read_only );
            execContext_->SetMappedRegion( mappedRegion_ );
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "SetupPrExecIPC failed: " << e.what() );
            ::exit( 1 );
        }
    }

    void InitProcessCompletionPipe()
    {
        int pipefd[2];
        if ( pipe( pipefd ) != -1 )
        {
            processCompletionPipe_ = pipefd[0];
            g_processCompletionPipe = pipefd[1];
        }
        else
        {
            PLOG_ERR( "InitProcessCompletionPipe: pipe creation failed " << strerror(errno) );
        }
    }

    void CloseProcessCompletionPipe()
    {
        if ( processCompletionPipe_ != -1 )
            close( processCompletionPipe_ );

        if ( g_processCompletionPipe != -1 )
            close( g_processCompletionPipe );
    }

private:
    string cfgDir_;
    bool isDaemon_;
    uid_t uid_;
    unsigned int numThread_;
    int processCompletionPipe_;

    std::vector<std::thread> worker_threads_;
    std::thread processCompletionThread_;

    boost::asio::io_service io_service_;

    std::shared_ptr< ConnectionAcceptor > acceptor_;

    std::shared_ptr< boost::interprocess::shared_memory_object > sharedMemPool_;
    std::shared_ptr< boost::interprocess::mapped_region > mappedRegion_;

    ExecContextPtr execContext_;
};

} // namespace worker


int main( int argc, char* argv[] )
{
    SetupSignalHandlers();
    SetupSignalMask();
    atexit( AtExit );

    try
    {
        bool isDaemon = false;
        uid_t uid = 0;
        unsigned int numThread = 0;
        std::string cfgDir, exeDir, resourcesDir;

        worker::g_isFork = false;

        // parse input command line options
        namespace po = boost::program_options;

        po::options_description descr;

        descr.add_options()
            ("num_thread", po::value<unsigned int>(), "Thread pool size")
            ("exe_dir", po::value<std::string>(), "Executable working directory")
            ("d", "Run as a daemon")
            ("u", po::value<uid_t>(), "Start as a specific non-root user")
            ("c", po::value<std::string>(), "Config file path")
            ("r", po::value<std::string>(), "Path to resources");

        po::variables_map vm;
        po::store( po::parse_command_line( argc, argv, descr ), vm );
        po::notify( vm );

        if ( vm.count( "d" ) )
        {
            isDaemon = true;
        }

        if ( vm.count( "u" ) )
        {
            uid = vm[ "u" ].as<uid_t>();
        }

        if ( vm.count( "num_thread" ) )
        {
            numThread = vm[ "num_thread" ].as<unsigned int>();
        }

        if ( vm.count( "exe_dir" ) )
        {
            exeDir = vm[ "exe_dir" ].as<std::string>();
        }

        if ( vm.count( "c" ) )
        {
            cfgDir = vm[ "c" ].as<std::string>();
        }

        if ( vm.count( "r" ) )
        {
            resourcesDir = vm[ "r" ].as<std::string>();
        }

        worker::ExecApplication app( exeDir, resourcesDir, cfgDir, isDaemon, uid, numThread );
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

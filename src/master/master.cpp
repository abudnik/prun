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

#include <iostream>
#include <list>
#include <thread>
#include <boost/asio.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/program_options.hpp>
#include <csignal>
#include <sys/wait.h>
#include "common/security.h"
#include "common/log.h"
#include "common/daemon.h"
#include "common/config.h"
#include "common/pidfile.h"
#include "common/uuid.h"
#include "common/service_locator.h"
#include "common/shared_library.h"
#include "common/history.h"
#include "ping.h"
#include "node_ping.h"
#include "job_manager.h"
#include "job_history.h"
#include "worker_manager.h"
#include "scheduler.h"
#include "job_sender.h"
#include "result_getter.h"
#include "command_sender.h"
#include "timeout_manager.h"
#include "cron_manager.h"
#include "admin.h"
#include "defines.h"
#include "test.h"
#ifdef HAVE_EXEC_INFO_H
#include "common/stack.h"
#endif

using namespace std;


namespace {

void InitWorkerManager( std::shared_ptr< master::WorkerManager > &mgr,
                        const std::string &exeDir, const std::string &cfgPath )
{
    string groupsDir, groupsPath;
    if ( cfgPath.empty() )
    {
        groupsDir = exeDir;
    }
    else
    {
        boost::filesystem::path p( cfgPath );
        boost::filesystem::path dir = p.parent_path();
        groupsDir = dir.string();
    }
    groupsPath = groupsDir + '/' + master::GROUPS_FILE_NAME;

    std::ifstream file( groupsPath.c_str() );
    if ( !file.is_open() )
    {
        PLOG_ERR( "InitWorkerManager: couldn't open " << groupsPath );
        return;
    }

    mgr->Initialize( groupsDir );

    std::string line;
    list< std::string > hosts;
    while( getline( file, line ) )
    {
        groupsPath = groupsDir + '/' + line;
        hosts.clear();
        if ( master::ReadHosts( groupsPath.c_str(), hosts ) )
        {
            mgr->AddWorkerGroup( line, hosts );
        }
    }
}

void SigHandler( int s )
{
    switch( s )
    {
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

    sigaction( SIGABRT, &sigHandler, nullptr );
    sigaction( SIGFPE,  &sigHandler, nullptr );
    sigaction( SIGBUS,  &sigHandler, nullptr );
    sigaction( SIGSEGV, &sigHandler, nullptr );
    sigaction( SIGILL,  &sigHandler, nullptr );
    sigaction( SIGSYS,  &sigHandler, nullptr );
    sigaction( SIGXCPU, &sigHandler, nullptr );
    sigaction( SIGXFSZ, &sigHandler, nullptr);
}

void AtExit()
{
    common::logger::ShutdownLogger();
}

void UserInteraction()
{
    while( !getchar() );
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

class MasterApplication
{
public:
    MasterApplication( const std::string &exeDir, const std::string &cfgPath, bool isDaemon, uid_t uid )
    : exeDir_( exeDir ),
     cfgPath_( cfgPath ),
     isDaemon_( isDaemon ),
     uid_( uid ),
     history_( nullptr )
    {
        masterId_ = common::GenerateUUID();
    }

    void Initialize()
    {
        SetupSignalHandlers();
        atexit( AtExit );

        // parse config & read some parameters
        common::Config &cfg = common::Config::Instance();
        if ( cfgPath_.empty() )
        {
            cfg.ParseConfig( exeDir_.c_str(), "master.cfg" );
        }
        else
        {
            cfg.ParseConfig( "", cfgPath_.c_str() );
        }
        ApplyDefaults( cfg );

        std::string logLevel = cfg.Get<std::string>( "log_level" );
        common::logger::InitLogger( isDaemon_, "pmaster", logLevel.c_str() );

        PLOG_DBG( "MasterApplication::Initialize: master_id=" << masterId_ );

        unsigned int numHeartbeatThread = 1;
        unsigned int numPingReceiverThread = cfg.Get<unsigned int>( "num_ping_receiver_thread" );
        unsigned int numJobSendThread = 1 + cfg.Get<unsigned int>( "num_job_send_thread" );
        unsigned int numResultGetterThread = 1 + cfg.Get<unsigned int>( "num_result_getter_thread" );
        unsigned int numCommandSendThread = 1 + cfg.Get<unsigned int>( "num_command_send_thread" );
        unsigned int numPingThread = numHeartbeatThread + numPingReceiverThread;

        // initialize main components
        common::ServiceLocator &serviceLocator = common::ServiceLocator::Instance();

        workerManager_.reset( new master::WorkerManager );
        serviceLocator.Register( static_cast< master::IWorkerManager* >( workerManager_.get() ) );
        InitWorkerManager( workerManager_, exeDir_, cfgPath_ );

        timeoutManager_.reset( new master::TimeoutManager( io_service_timeout_ ) );

        cronManager_.reset( new master::CronManager( io_service_cron_ ) );
        serviceLocator.Register( static_cast< master::ICronManager* >( cronManager_.get() ) );

        jobManager_.reset( new master::JobManager );
        jobManager_->SetMasterId( masterId_ ).SetExeDir( exeDir_ ).SetTimeoutManager( timeoutManager_.get() );
        serviceLocator.Register( static_cast< master::IJobManager* >( jobManager_.get() ) );

        scheduler_.reset( new master::Scheduler );
        serviceLocator.Register( static_cast< master::IScheduler* >( scheduler_.get() ) );

        InitHistory();

        jobHistory_.reset( new master::JobHistory( history_ ) );
        serviceLocator.Register( static_cast< master::IJobEventReceiver* >( jobHistory_.get() ) );
        jobHistory_->GetJobs();

        timeoutManager_->Start();
        worker_threads_.push_back( std::thread( ThreadFun, &io_service_timeout_ ) );

        cronManager_->Start();
        worker_threads_.push_back( std::thread( ThreadFun, &io_service_cron_ ) );

        // start ping from nodes receiver threads
        pingReceiver_.reset( new master::PingReceiverBoost( io_service_ping_ ) );
        pingReceiver_->Start();

        // start node pinger
        int heartbeatDelay = cfg.Get<int>( "heartbeat_delay" );
        int maxDroped = cfg.Get<int>( "heartbeat_max_droped" );
        pinger_.reset( new master::PingerBoost( io_service_ping_, heartbeatDelay, maxDroped ) );
        pinger_->StartPing();

        // create thread pool for pingers
        for( unsigned int i = 0; i < numPingThread; ++i )
        {
            worker_threads_.push_back( std::thread( ThreadFun, &io_service_ping_ ) );
        }

        // start job sender thread
        int maxSimultSendingJobs = cfg.Get<int>( "max_simult_sending_jobs" );
        jobSender_.reset(
            new master::JobSenderBoost( io_service_senders_, timeoutManager_.get(),
                                        maxSimultSendingJobs )
        );
        jobSender_->Start();

        // create thread pool for job senders
        for( unsigned int i = 0; i < numJobSendThread; ++i )
        {
            worker_threads_.push_back( std::thread( ThreadFun, &io_service_senders_ ) );
        }

        // start result getter
        int maxSimultResultGetters = cfg.Get<int>( "max_simult_result_getters" );
        resultGetter_.reset( new master::ResultGetterBoost( io_service_getters_, maxSimultResultGetters ) );
        resultGetter_->Start();

        // create thread pool for job result getters
        for( unsigned int i = 0; i < numResultGetterThread; ++i )
        {
            worker_threads_.push_back( std::thread( ThreadFun, &io_service_getters_ ) );
        }

        // start command sender
        int maxSimultCommandSend = cfg.Get<int>( "max_simult_command_send" );
        commandSender_.reset(
            new master::CommandSenderBoost( io_service_command_send_, timeoutManager_.get(),
                                            maxSimultCommandSend )
        );
        commandSender_->Start();

        // create thread pool for command senders
        for( unsigned int i = 0; i < numCommandSendThread; ++i )
        {
            worker_threads_.push_back( std::thread( ThreadFun, &io_service_command_send_ ) );
        }

        // create thread for admin connections
        adminConnection_.reset( new master::AdminConnection( io_service_admin_ ) );
        worker_threads_.push_back( std::thread( ThreadFun, &io_service_admin_ ) );
    }

    void Shutdown()
    {
        PLOG_DBG( "MasterApplication::Shutdown" );

        // stop io services
        io_service_admin_.stop();
        io_service_command_send_.stop();
        io_service_getters_.stop();
        io_service_senders_.stop();
        io_service_ping_.stop();

        if ( timeoutManager_ )
            timeoutManager_->Stop();

        if ( cronManager_ )
            cronManager_->Stop();

        if ( pinger_ )
            pinger_->Stop();

        if ( jobSender_ )
            jobSender_->Stop();

        if ( resultGetter_ )
            resultGetter_->Stop();

        if ( commandSender_ )
            commandSender_->Stop();

        if ( history_ )
            history_->Shutdown();

        // stop thread pool
        for( auto &t : worker_threads_ )
            t.join();

        // shutdown managers
        if ( jobManager_ )
            jobManager_->Shutdown();

        if ( workerManager_ )
            workerManager_->Shutdown();

        if ( history_ )
            (*historyDestroy_)( history_ );

        common::ServiceLocator::Instance().UnregisterAll();
    }

    void Run()
    {
        PLOG_DBG( "MasterApplication::Run" );

        common::Config &cfg = common::Config::Instance();

        string pidfilePath = cfg.Get<string>( "pidfile" );
        if ( pidfilePath[0] != '/' )
        {
            pidfilePath = exeDir_ + '/' + pidfilePath;
        }

        common::Pidfile pidfile( pidfilePath.c_str() );

        common::ImpersonateOrExit( uid_ );

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
            sigprocmask( SIG_BLOCK, &waitset, nullptr );
            while( 1 )
            {
                int ret = sigwait( &waitset, &sig );
                if ( ret == EINTR )
                    continue;
                if ( !ret )
                    break;
                PLOG_ERR( "MasterApplication::Run: sigwait failed: " << strerror(ret) );
            }
        }
    }

private:
    void ApplyDefaults( common::Config &cfg ) const
    {
        cfg.Insert( "node_port", master::NODE_PORT );
        cfg.Insert( "node_ping_port", master::NODE_UDP_PORT );
        cfg.Insert( "master_ping_port", master::MASTER_UDP_PORT );
        cfg.Insert( "master_admin_port", master::MASTER_ADMIN_PORT );
    }

    void InitHistory()
    {
        common::Config &cfg = common::Config::Instance();
        try
        {
            std::string historyLibPath = cfg.Get<std::string>( "history_library" );
            std::string historyConfigPath = cfg.Get<std::string>( "history_config" );

            if ( historyLibPath.empty() )
            {
                PLOG_DBG( "MasterApplication::InitHistory: history shared library name is empty" );
                return;
            }

            if ( historyLibrary_.Load( historyLibPath.c_str() ) )
            {
                typedef common::IHistory *HistoryCreateType( int interfaceVersion );
                auto CreateHistory = reinterpret_cast<HistoryCreateType *>( historyLibrary_.GetFunction( "CreateHistory" ) );
                if ( !CreateHistory )
                {
                    PLOG_ERR( "MasterApplication::InitHistory: couldn't resolve 'CreateHistory' function" );
                    return;
                }
                historyDestroy_ = reinterpret_cast< void (*)(const common::IHistory *) >( historyLibrary_.GetFunction( "DestroyHistory" ) );
                if ( !historyDestroy_ )
                {
                    PLOG_ERR( "MasterApplication::InitHistory: couldn't resolve 'DestroyHistory' function" );
                    return;
                }

                history_ = (*CreateHistory)( common::HISTORY_VERSION );
                if ( history_ )
                {
                    history_->Initialize( historyConfigPath.c_str() );

                    common::ServiceLocator &serviceLocator = common::ServiceLocator::Instance();
                    serviceLocator.Register( history_ );
                }
                else
                {
                    PLOG_ERR( "MasterApplication::InitHistory: CreateHistory failed: interface version=" << common::HISTORY_VERSION );
                }
            }
            else
            {
                PLOG_ERR( "MasterApplication::InitHistory: couldn't load history library '" << historyLibPath << '\'' );
            }
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "MasterApplication::InitHistory: " << e.what() );
        }
    }

private:
    std::string exeDir_;
    std::string cfgPath_;
    bool isDaemon_;
    uid_t uid_;
    std::string masterId_;

    common::SharedLibrary historyLibrary_;

    std::vector<std::thread> worker_threads_;

    boost::asio::io_service io_service_timeout_;
    boost::asio::io_service io_service_cron_;
    boost::asio::io_service io_service_ping_;
    boost::asio::io_service io_service_senders_;
    boost::asio::io_service io_service_getters_;
    boost::asio::io_service io_service_command_send_;
    boost::asio::io_service io_service_admin_;

    std::shared_ptr< master::JobManager > jobManager_;
    std::shared_ptr< master::JobHistory > jobHistory_;
    std::shared_ptr< master::WorkerManager > workerManager_;
    std::shared_ptr< master::Scheduler > scheduler_;
    std::shared_ptr< master::TimeoutManager > timeoutManager_;
    std::shared_ptr< master::CronManager > cronManager_;

    common::IHistory *history_;
    void ( *historyDestroy_ )( const common::IHistory * );

    std::shared_ptr< master::PingReceiver > pingReceiver_;
    std::shared_ptr< master::Pinger > pinger_;
    std::shared_ptr< master::JobSender > jobSender_;
    std::shared_ptr< master::ResultGetter > resultGetter_;
    std::shared_ptr< master::CommandSender > commandSender_;
    std::shared_ptr< master::AdminConnection > adminConnection_;
};

} // anonymous namespace

int main( int argc, char* argv[] )
{
    try
    {
        bool isDaemon = false;
        std::string exeDir = boost::filesystem::system_complete( argv[0] ).branch_path().string();
        std::string cfgPath;
        uid_t uid = 0;

        // parse input command line options
        namespace po = boost::program_options;

        po::options_description descr;

        descr.add_options()
            ("help", "Print help")
            ("d", "Run as a daemon")
            ("s", "Stop daemon")
            ("u", po::value<uid_t>(), "Start as a specific non-root user")
            ("c", po::value<std::string>(), "Config file path");

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
            return common::StopDaemon( "pmaster" );
        }

        if ( vm.count( "d" ) )
        {
            common::StartAsDaemon();
            isDaemon = true;
        }

        if ( vm.count( "c" ) )
        {
            cfgPath = vm[ "c" ].as<std::string>();
        }

        if ( vm.count( "u" ) )
        {
            uid = vm[ "u" ].as<uid_t>();
        }

        MasterApplication app( exeDir, cfgPath, isDaemon, uid );
        app.Initialize();

#ifdef _DEBUG
        master::IJobManager *jobManager = common::GetService< master::IJobManager >();
        const std::string &jobsDir = jobManager->GetJobsDir();
        master::RunTests( jobsDir );
#endif

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

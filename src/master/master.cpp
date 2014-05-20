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
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/program_options.hpp>
#include <csignal>
#include <sys/wait.h>
#include "common/log.h"
#include "common/daemon.h"
#include "common/config.h"
#include "common/pidfile.h"
#include "common/uuid.h"
#include "common/service_locator.h"
#include "ping.h"
#include "node_ping.h"
#include "job_manager.h"
#include "job_history.h"
#include "dbconnection.h"
#include "worker_manager.h"
#include "scheduler.h"
#include "job_sender.h"
#include "result_getter.h"
#include "command_sender.h"
#include "timeout_manager.h"
#include "admin.h"
#include "defines.h"
#include "test.h"
#ifdef HAVE_EXEC_INFO_H
#include "common/stack.h"
#endif

using namespace std;


namespace {

void InitWorkerManager( boost::shared_ptr< master::WorkerManager > &mgr,
                        const std::string &exeDir, const std::string &cfgPath )
{
    string hostsDir, hostsPath;
    if ( cfgPath.empty() )
    {
        hostsDir = exeDir;
    }
    else
    {
        boost::filesystem::path p( cfgPath );
        boost::filesystem::path dir = p.parent_path();
        hostsDir = dir.string();
    }
    hostsPath = hostsDir + '/' + master::HOSTS_FILE_NAME;

    std::ifstream file( hostsPath.c_str() );
    if ( !file.is_open() )
    {
        PLOG_ERR( "InitWorkerManager: couldn't open " << hostsPath );
        return;
    }

    mgr->Initialize( hostsDir );

    std::string line;
    list< std::string > hosts;
    while( getline( file, line ) )
    {
        hostsPath = hostsDir + '/' + line;
        hosts.clear();
        if ( master::ReadHosts( hostsPath.c_str(), hosts ) )
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

    sigaction( SIGABRT, &sigHandler, NULL );
    sigaction( SIGFPE,  &sigHandler, NULL );
    sigaction( SIGBUS,  &sigHandler, NULL );
    sigaction( SIGSEGV, &sigHandler, NULL );
    sigaction( SIGILL,  &sigHandler, NULL );
    sigaction( SIGSYS,  &sigHandler, NULL );
    sigaction( SIGXCPU, &sigHandler, NULL );
    sigaction( SIGXFSZ, &sigHandler, NULL );
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
    MasterApplication( const std::string &exeDir, const std::string &cfgPath, bool isDaemon )
    : exeDir_( exeDir ),
     cfgPath_( cfgPath ),
     isDaemon_( isDaemon )
    {
        masterId_ = common::GenerateUUID();
    }

    void Initialize()
    {
        common::logger::InitLogger( isDaemon_, "pmaster" );

        //PLOG( "master_id= " << masterId_ );

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

        unsigned int numHeartbeatThread = 1;
        unsigned int numPingReceiverThread = cfg.Get<unsigned int>( "num_ping_receiver_thread" );
        unsigned int numJobSendThread = 1 + cfg.Get<unsigned int>( "num_job_send_thread" );
        unsigned int numResultGetterThread = 1 + cfg.Get<unsigned int>( "num_result_getter_thread" );
        unsigned int numCommandSendThread = 1 + cfg.Get<unsigned int>( "num_command_send_thread" );
        unsigned int numPingThread = numHeartbeatThread + numPingReceiverThread;

        std::string masterdb = cfg.Get<std::string>( "masterdb" );

        // initialize main components
        common::ServiceLocator &serviceLocator = common::ServiceLocator::Instance();

        workerManager_.reset( new master::WorkerManager );
        serviceLocator.Register( static_cast< master::IWorkerManager* >( workerManager_.get() ) );
        InitWorkerManager( workerManager_, exeDir_, cfgPath_ );

        timeoutManager_.reset( new master::TimeoutManager( io_service_timeout_ ) );

        jobManager_.reset( new master::JobManager );
        jobManager_->SetMasterId( masterId_ ).SetExeDir( exeDir_ ).SetTimeoutManager( timeoutManager_.get() );
        serviceLocator.Register( static_cast< master::IJobManager* >( jobManager_.get() ) );

        scheduler_.reset( new master::Scheduler );
        serviceLocator.Register( static_cast< master::IScheduler* >( scheduler_.get() ) );

        dbConnection_.reset( new master::DbHistoryConnection( io_service_db_ ) );
        jobHistory_.reset( new master::JobHistory( dbConnection_.get() ) );
        serviceLocator.Register( static_cast< master::IJobEventReceiver* >( jobHistory_.get() ) );
        if ( dbConnection_->Connect( masterdb, master::MASTERDB_PORT ) )
        {
            jobHistory_->GetJobs();
        }

        timeoutManager_->Start();
        worker_threads_.create_thread(
            boost::bind( &ThreadFun, &io_service_timeout_ )
        );

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
            worker_threads_.create_thread(
                boost::bind( &ThreadFun, &io_service_ping_ )
            );
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
            worker_threads_.create_thread(
                boost::bind( &ThreadFun, &io_service_senders_ )
            );
        }

        // start result getter
        int maxSimultResultGetters = cfg.Get<int>( "max_simult_result_getters" );
        resultGetter_.reset( new master::ResultGetterBoost( io_service_getters_, maxSimultResultGetters ) );
        resultGetter_->Start();

        // create thread pool for job result getters
        for( unsigned int i = 0; i < numResultGetterThread; ++i )
        {
            worker_threads_.create_thread(
                boost::bind( &ThreadFun, &io_service_getters_ )
            );
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
            worker_threads_.create_thread(
                boost::bind( &ThreadFun, &io_service_command_send_ )
            );
        }

        // create thread for admin connections
        adminConnection_.reset( new master::AdminConnection( io_service_admin_ ) );
        worker_threads_.create_thread(
            boost::bind( &ThreadFun, &io_service_admin_ )
        );
    }

    void Shutdown()
    {
        // stop io services
        io_service_admin_.stop();
        io_service_command_send_.stop();
        io_service_getters_.stop();
        io_service_senders_.stop();
        io_service_ping_.stop();

        if ( timeoutManager_ )
            timeoutManager_->Stop();

        if ( pinger_ )
            pinger_->Stop();

        if ( jobSender_ )
            jobSender_->Stop();

        if ( resultGetter_ )
            resultGetter_->Stop();

        if ( commandSender_ )
            commandSender_->Stop();

        // disconnect from masterdb
        dbConnection_->Shutdown();

        // stop thread pool
        worker_threads_.join_all();

        // shutdown managers
        if ( jobManager_ )
            jobManager_->Shutdown();

        if ( workerManager_ )
            workerManager_->Shutdown();

        common::ServiceLocator::Instance().UnregisterAll();
    }

    void Run()
    {
        common::Config &cfg = common::Config::Instance();

        string pidfilePath = cfg.Get<string>( "pidfile" );
        if ( pidfilePath[0] != '/' )
        {
            pidfilePath = exeDir_ + '/' + pidfilePath;
        }

        common::Pidfile pidfile( pidfilePath.c_str() );

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
			sigprocmask( SIG_BLOCK, &waitset, NULL );
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
    std::string exeDir_;
    std::string cfgPath_;
    bool isDaemon_;
    std::string masterId_;

    boost::thread_group worker_threads_;

    boost::asio::io_service io_service_timeout_;
    boost::asio::io_service io_service_ping_;
    boost::asio::io_service io_service_senders_;
    boost::asio::io_service io_service_getters_;
    boost::asio::io_service io_service_command_send_;
    boost::asio::io_service io_service_admin_;
    boost::asio::io_service io_service_db_;

    boost::shared_ptr< master::JobManager > jobManager_;
    boost::shared_ptr< master::JobHistory > jobHistory_;
    boost::shared_ptr< master::DbHistoryConnection > dbConnection_;
    boost::shared_ptr< master::WorkerManager > workerManager_;
    boost::shared_ptr< master::Scheduler > scheduler_;
    boost::shared_ptr< master::TimeoutManager > timeoutManager_;

    boost::shared_ptr< master::PingReceiver > pingReceiver_;
    boost::shared_ptr< master::Pinger > pinger_;
    boost::shared_ptr< master::JobSender > jobSender_;
    boost::shared_ptr< master::ResultGetter > resultGetter_;
    boost::shared_ptr< master::CommandSender > commandSender_;
    boost::shared_ptr< master::AdminConnection > adminConnection_;
};

} // anonymous namespace

int main( int argc, char* argv[] )
{
    try
    {
        bool isDaemon = false;
        std::string exeDir = boost::filesystem::system_complete( argv[0] ).branch_path().string();
        std::string cfgPath;

        // parse input command line options
        namespace po = boost::program_options;
        
        po::options_description descr;

        descr.add_options()
            ("help", "Print help")
            ("d", "Run as a daemon")
            ("s", "Stop daemon")
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

        MasterApplication app( exeDir, cfgPath, isDaemon );
        app.Initialize();

#ifdef _DEBUG
        master::IJobManager *jobManager = common::ServiceLocator::Instance().Get< master::IJobManager >();
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

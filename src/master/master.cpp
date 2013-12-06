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
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <csignal>
#include <sys/wait.h>
#include "common/log.h"
#include "common/daemon.h"
#include "common/config.h"
#include "common/pidfile.h"
#include "ping.h"
#include "node_ping.h"
#include "job_manager.h"
#include "worker_manager.h"
#include "scheduler.h"
#include "job_sender.h"
#include "result_getter.h"
#include "command_sender.h"
#include "timeout_manager.h"
#include "admin.h"
#include "defines.h"
#include "test.h"

using namespace std;


namespace {

void InitWorkerManager( const std::string &exeDir )
{
    string hostsPath = exeDir + '/' + master::HOSTS_FILE_NAME;

    std::ifstream file( hostsPath.c_str() );
    if ( !file.is_open() )
    {
        PLOG( "InitWorkerManager: couldn't open " << hostsPath );
        return;
    }

    master::WorkerManager &mgr = master::WorkerManager::Instance();
    std::string line;
    list< std::string > hosts;
    while( getline( file, line ) )
    {
        hostsPath = exeDir + '/' + line;
        hosts.clear();
        if ( master::ReadHosts( hostsPath.c_str(), hosts ) )
        {
            mgr.AddWorkerGroup( line, hosts );
        }
    }
}

void AtExit()
{
    common::JsonRpc::Instance().Shutdown();
    master::WorkerManager::Instance().Shutdown();
    master::JobManager::Instance().Shutdown();
    master::Scheduler::Instance().Shutdown();

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
        PLOG( "ThreadFun: " << e.what() );
    }
}

class MasterApplication
{
public:
    MasterApplication(  const std::string &exeDir, bool isDaemon )
    : exeDir_( exeDir ),
     isDaemon_( isDaemon )
    {
        masterId_ = boost::uuids::random_generator()();
    }

    void Initialize()
    {
        common::logger::InitLogger( isDaemon_, "Master" );

        //PLOG( "master_id= " << masterId_ );

        common::Config &cfg = common::Config::Instance();
        cfg.ParseConfig( exeDir_.c_str(), "master.cfg" );

        unsigned int numHeartbeatThread = 1;
        unsigned int numPingReceiverThread = cfg.Get<unsigned int>( "num_ping_receiver_thread" );
        unsigned int numJobSendThread = 1 + cfg.Get<unsigned int>( "num_job_send_thread" );
        unsigned int numResultGetterThread = 1 + cfg.Get<unsigned int>( "num_result_getter_thread" );
        unsigned int numCommandSendThread = 1 + cfg.Get<unsigned int>( "num_command_send_thread" );
        unsigned int numPingThread = numHeartbeatThread + numPingReceiverThread;

        InitWorkerManager( exeDir_ );

        timeoutManager_.reset( new master::TimeoutManager( io_service_timeout_ ) );

        std::string masterId = boost::lexical_cast< std::string >( masterId_ );
        master::JobManager::Instance().Initialize( masterId, exeDir_, timeoutManager_.get() );

        master::Scheduler::Instance();
        master::AdminSession::InitializeRpcHandlers();

        atexit( AtExit );

        timeoutManager_->Start();
        worker_threads_.create_thread(
            boost::bind( &ThreadFun, &io_service_timeout_ )
        );

        // start ping from nodes receiver threads
        pingReceiver_.reset( new master::PingReceiverBoost( io_service_ping_ ) );
        pingReceiver_->Start();

        // start node pinger
        int heartbeatTimeout = cfg.Get<int>( "heartbeat_timeout" );
        int maxDroped = cfg.Get<int>( "heartbeat_max_droped" );
        pinger_.reset( new master::PingerBoost( io_service_ping_, heartbeatTimeout, maxDroped ) );
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

        timeoutManager_->Stop();
        pinger_->Stop();
        jobSender_->Stop();
        resultGetter_->Stop();
        commandSender_->Stop();

        // stop thread pool
        worker_threads_.join_all();
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
                if ( !ret )
                    break;
                PLOG( "main(): sigwait failed: " << strerror(errno) );
            }
        }
    }

private:
    std::string exeDir_;
    bool isDaemon_;
    boost::uuids::uuid masterId_;

    boost::thread_group worker_threads_;

    boost::asio::io_service io_service_timeout_;
    boost::asio::io_service io_service_ping_;
    boost::asio::io_service io_service_senders_;
    boost::asio::io_service io_service_getters_;
    boost::asio::io_service io_service_command_send_;
    boost::asio::io_service io_service_admin_;

    boost::shared_ptr< master::TimeoutManager > timeoutManager_;
    boost::shared_ptr< master::PingReceiver > pingReceiver_;
    boost::shared_ptr< master::Pinger > pinger_;
    boost::shared_ptr< master::JobSender > jobSender_;
    boost::shared_ptr< master::ResultGetter > resultGetter_;
    boost::shared_ptr< master::CommandSender > commandSender_;
    boost::shared_ptr< master::AdminConnection > adminConnection_;
};

} // anonymous namespace

int main( int argc, char* argv[], char **envp )
{
    try
    {
        bool isDaemon = false;
        std::string exeDir = boost::filesystem::system_complete( argv[0] ).branch_path().string();

        // parse input command line options
        namespace po = boost::program_options;
        
        po::options_description descr;

        descr.add_options()
            ("help", "Print help")
            ("d", "Run as a daemon")
            ("stop", "Stop daemon");
        
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
            return common::StopDaemon( "master" );
        }

        if ( vm.count( "d" ) )
        {
            common::StartAsDaemon();
            isDaemon = true;
        }

        MasterApplication app( exeDir, isDaemon );
        app.Initialize();

        master::RunTests( exeDir );

        app.Run();

        app.Shutdown();
    }
    catch( std::exception &e )
    {
        cout << "Exception: " << e.what() << endl;
        PLOG( "Exception: " << e.what() );
    }

    PLOG( "stopped" );

    return 0;
}

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
#include <fstream> // for RunTests() only
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
#include "ping.h"
#include "node_ping.h"
#include "job_manager.h"
#include "worker_manager.h"
#include "defines.h"

using namespace std;


namespace master {

bool isDaemon;
unsigned int numThread;
string exeDir;

} // namespace master

namespace {

void InitWorkerManager()
{
    string hostsPath = master::exeDir + '/' + master::HOSTS_FILE_NAME;
    list< string > hosts;

    if ( master::ReadHosts( hostsPath.c_str(), hosts ) )
    {
		master::WorkerManager &mgr = master::WorkerManager::Instance();
		mgr.Initialize( hosts );
    }
}

void InitJobManager()
{
    master::JobManager &mgr = master::JobManager::Instance();
    mgr.SetExeDir( master::exeDir );
}

void RunTests()
{
    // read job description from file
    string filePath = master::exeDir + "/test/test.job";
    ifstream file( filePath.c_str() );
    if ( !file.is_open() )
    {
        PS_LOG( "RunTests: couldn't open " << filePath );
        return;
    }
    string job, line;
    while( getline( file, line ) )
        job += line;
    // add job to job queue
    master::JobManager::Instance().PushJob( job );
}

void AtExit()
{
    master::WorkerManager::Instance().Shutdown();

	python_server::logger::ShutdownLogger();
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
		PS_LOG( "ThreadFun: " << e.what() );
	}
}

} // anonymous namespace

int main( int argc, char* argv[], char **envp )
{
	try
	{
        // initialization
        master::isDaemon = false;

        master::exeDir = boost::filesystem::system_complete( argv[0] ).branch_path().string();

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
			return python_server::StopDaemon( "master" );
		}

		if ( vm.count( "d" ) )
		{
			python_server::StartAsDaemon();
			master::isDaemon = true;
		}

        int numPingThread = 1;
		int numPingReceiverThread = 2;
        master::numThread = numPingThread + numPingReceiverThread;

		python_server::logger::InitLogger( master::isDaemon, "Master" );

        python_server::Config &cfg = python_server::Config::Instance();
		cfg.ParseConfig( master::exeDir.c_str(), "master.cfg" );

        string pidfilePath = cfg.Get<string>( "pidfile" );
        if ( pidfilePath[0] != '/' )
        {
            pidfilePath = master::exeDir + '/' + pidfilePath;
        }
        python_server::Pidfile pidfile( pidfilePath.c_str() );

        InitWorkerManager();
        InitJobManager();

		atexit( AtExit );

		boost::asio::io_service io_service;
        boost::scoped_ptr<boost::asio::io_service::work> work(
            new boost::asio::io_service::work( io_service ) );

		// create thread pool
		boost::thread_group worker_threads;
		for( unsigned int i = 0; i < master::numThread; ++i )
		{
		    worker_threads.create_thread(
				boost::bind( &ThreadFun, &io_service )
			);
		}

        int pingTimeout = cfg.Get<int>( "ping_timeout" );
        int maxDroped = cfg.Get<int>( "ping_max_droped" );
        boost::scoped_ptr< master::Pinger > pinger(
            new master::PingerBoost( master::WorkerManager::Instance(),
                                     io_service, pingTimeout, maxDroped ) );

		for( int i = 0; i < numPingThread; ++i )
			pinger->StartPing();

        boost::scoped_ptr< master::PingReceiver > pingReceiver(
            new master::PingReceiverBoost( master::WorkerManager::Instance(),
										   io_service ) );

		for( int i = 0; i < numPingReceiverThread; ++i )
			pingReceiver->Start();

        RunTests();

		if ( !master::isDaemon )
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
			sigwait( &waitset, &sig );
		}

        pinger->Stop();

        work.reset();
		io_service.stop();

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

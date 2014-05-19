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
#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/program_options.hpp>
#include <csignal>
#include <sys/wait.h>
#include "common/log.h"
#include "common/daemon.h"
#include "common/config.h"
#include "common/configure.h"
#include "common/pidfile.h"
#include "common/service_locator.h"
#include "session.h"
#include "defines.h"
#ifdef HAVE_LEVELDB_H
#include "dblevel.h"
#else
#include "dbmemory.h"
#endif
#ifdef HAVE_EXEC_INFO_H
#include "common/stack.h"
#endif

using namespace std;


namespace {

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
            break;
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

class MasterDbApplication
{
public:
    MasterDbApplication( const std::string &exeDir, const std::string &cfgPath, bool isDaemon )
    : exeDir_( exeDir ),
     cfgPath_( cfgPath ),
     isDaemon_( isDaemon )
    {}

    void Initialize()
    {
        common::logger::InitLogger( isDaemon_, "pmasterdb" );

        SetupSignalHandlers();
        atexit( AtExit );

        // parse config & read some parameters
        common::Config &cfg = common::Config::Instance();
        if ( cfgPath_.empty() )
        {
            cfg.ParseConfig( exeDir_.c_str(), "masterdb.cfg" );
        }
        else
        {
            cfg.ParseConfig( "", cfgPath_.c_str() );
        }

        // initialize main components
        dbClient_.Initialize( exeDir_ );

        common::ServiceLocator &serviceLocator = common::ServiceLocator::Instance();
        serviceLocator.Register( (masterdb::IDAO*)&dbClient_ );

        acceptor_.reset( new masterdb::ConnectionAcceptor( io_service_, masterdb::MASTERDB_PORT ) );

        workerThread_.reset(
            new boost::thread( boost::bind( &ThreadFun, &io_service_ ) )
        );
    }

    void Shutdown()
    {
        io_service_.stop();
        acceptor_->Stop();

        if ( !workerThread_->timed_join( boost::posix_time::seconds( 2 ) ) )
        {
            PLOG_WRN( "MasterDbApplication::Shutdown: timed_join() timeout" );
        }

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

    boost::scoped_ptr< boost::thread > workerThread_;
    boost::asio::io_service io_service_;

    boost::scoped_ptr< masterdb::ConnectionAcceptor > acceptor_;

#ifdef HAVE_LEVELDB_H
    masterdb::DbLevel dbClient_;
#else
    masterdb::DbInMemory dbClient_;
#endif
};

} // anonymous namespace

int main( int argc, char* argv[], char **envp )
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
            return common::StopDaemon( "pmasterdb" );
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

        MasterDbApplication app( exeDir, cfgPath, isDaemon );
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

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
#include <boost/filesystem/operations.hpp>
#include <boost/program_options.hpp>
#include "common/log.h"
#include "common/daemon.h"
#include "common/config.h"
#include "common/pidfile.h"
#include "job.h"
#include "worker.h"

using namespace std;


namespace master {

bool isDaemon;
string exeDir;

} // namespace master


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

		python_server::logger::InitLogger( master::isDaemon, "Master" );

		python_server::Config::Instance().ParseConfig( master::exeDir.c_str() );

        string pidfilePath = python_server::Config::Instance().Get<string>( "pidfile" );
        if ( pidfilePath[0] != '/' )
        {
            pidfilePath = master::exeDir + '/' + pidfilePath;
        }
        python_server::Pidfile pidfile( pidfilePath.c_str() );
	}
	catch( std::exception &e )
	{
		cout << "Exception: " << e.what() << endl;
		PS_LOG( "Exception: " << e.what() );
	}

    PS_LOG( "stopped" );

    return 0;
}

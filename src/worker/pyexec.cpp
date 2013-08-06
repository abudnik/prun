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
#include <unistd.h>
#include <csignal>
#include <sys/wait.h>
#include <sys/prctl.h>
#include "request.h"
#include "common.h"
#include "common/log.h"
#include "common/config.h"


using namespace std;
using boost::asio::ip::tcp;

namespace python_server {

bool isDaemon;
bool isFork;
uid_t uid;
unsigned int numThread;
string exeDir;
string nodeScriptPath;
string shmemPath;

boost::interprocess::shared_memory_object *sharedMemPool;
boost::interprocess::mapped_region *mappedRegion; 

struct ThreadParams
{
    int fifofd;
    string fifoName;
	pid_t pid;
};

typedef std::map< boost::thread::id, ThreadParams > ThreadInfo;
ThreadInfo threadInfo;


class Job
{
public:
	template< typename T >
	void ParseRequest( Request<T> &request )
	{
		const std::string &requestStr = request.GetString();
	    
		std::stringstream ss;
		ss << requestStr;

		boost::property_tree::ptree ptree;
		boost::property_tree::read_json( ss, ptree );

		jobId_ = ptree.get<int>( "id" );
	    scriptLength_ = ptree.get<unsigned int>( "len" );
		language_ = ptree.get<std::string>( "lang" );
	}

	void GetResponse( std::string &response )
	{
		std::stringstream ss;
		boost::property_tree::ptree ptree;

		ptree.put( "err", errCode_ );

		boost::property_tree::write_json( ss, ptree, false );
        size_t responseLength = ss.str().size();
        response = boost::lexical_cast< std::string >( responseLength );
        response += '\n';
		response += ss.str();
	}

	void OnError( int err )
	{
		errCode_ = err;
	}

	int GetJobId() const { return jobId_; }
	unsigned int GetScriptLength() const { return scriptLength_; }
	const std::string &GetScriptLanguage() const { return language_; }

private:
	int jobId_;
	unsigned int scriptLength_;
	int errCode_;
	std::string language_;
};

class ScriptExec
{
public:
	virtual void Execute( Job *job ) = 0;
	virtual ~ScriptExec() {}
};

class PythonExec : public ScriptExec
{
public:
	PythonExec()
	{
		pythonExePath_ = Config::Instance().Get<string>( "python" );
	}

	virtual void Execute( Job *job )
	{
		job_ = job;

		pid_t pid = DoFork();
        if ( pid > 0 )
            return;

		std::stringstream ss, ss2;

		ss << job->GetScriptLength();
		string scriptLength = ss.str();

	    size_t offset = job->GetJobId() * SHMEM_BLOCK_SIZE;
        ss2 << offset;
        string shmemOffset = ss2.str();

        ThreadParams &threadParams = threadInfo[ boost::this_thread::get_id() ];

        int ret = execl( pythonExePath_.c_str(), "python",
                         nodeScriptPath.c_str(),
                         threadParams.fifoName.c_str(), shmemPath.c_str(),
                         scriptLength.c_str(), shmemOffset.c_str(), NULL );
		if ( ret < 0 )
		{
			PS_LOG( "HandleRequest: execl failed: " << strerror(errno) );
        }
		::exit( 1 );
	}

    pid_t DoFork()
	{
		pid_t pid = fork();

		if ( pid > 0 )
		{
			//PS_LOG( "wait child " << pid );
			ThreadParams &threadParams = threadInfo[ boost::this_thread::get_id() ];
		    threadParams.pid = pid;

            int fifo = threadParams.fifofd;
            if ( fifo != -1 )
            {
                sigset_t sigset, oldset;
                sigemptyset( &sigset );
                sigaddset( &sigset, SIGCHLD );
                sigprocmask( SIG_BLOCK, &sigset, &oldset );

                pollfd pfd[1];
                pfd[0].fd = fifo;
                pfd[0].events = POLLIN;

				int errCode = -1;
                int ret = poll( pfd, 1, -1 );
                if ( ret > 0 )
                {
                    ret = read( fifo, &errCode, sizeof( errCode ) );
                    if ( ret <= 0 )
                    {
                        PS_LOG( "read fifo failed: " << strerror(errno) );
                    }
                }
                else
                {
                    PS_LOG( "ppoll failed: " << strerror(errno) );
                }
				job_->OnError( errCode );

                sigprocmask( SIG_BLOCK, &oldset, NULL );
            }
            else
            {
                PS_LOG( "DoFork: pipe not opened" );
                job_->OnError( -1 );
            }
			//PS_LOG( "wait child done " << pid );
		}
		else
		if ( pid == 0 )
		{
			isFork = true;
			prctl( PR_SET_PDEATHSIG, SIGHUP );
		}
		else
		{
			PS_LOG( "DoFork: fork() failed " << strerror(errno) );
		}

		return pid;
	}

private:
	Job *job_;
	std::string pythonExePath_;
};

class ExecCreator
{
public:
	virtual ScriptExec *Create( const std::string &language )
	{
		if ( language == "python" )
			return new PythonExec();
		return NULL;
	}
};

class Session : public boost::enable_shared_from_this< Session >
{
	typedef boost::array< char, 1024 > BufferType;

public:
	Session( boost::asio::io_service &io_service )
	: socket_( io_service )
	{
	}

	virtual ~Session()
	{
		cout << "E: ~Session()" << endl;
	}

	void Start()
	{
	    memset( buffer_.c_array(), 0, buffer_.size() );
		socket_.async_read_some( boost::asio::buffer( buffer_ ),
								 boost::bind( &Session::FirstRead, shared_from_this(),
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
										 boost::bind( &Session::FirstRead, shared_from_this(),
													  boost::asio::placeholders::error,
													  boost::asio::placeholders::bytes_transferred ) );
				return;
			}
			if ( ret < 0 )
			{
				job_.OnError( ret );
				WriteResponse();
				return;
			}
		}
		else
		{
			PS_LOG( "Session::FirstRead error=" << error.value() );
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
										 boost::bind( &Session::HandleRead, shared_from_this(),
													boost::asio::placeholders::error,
													boost::asio::placeholders::bytes_transferred ) );
			}
			else
			{
				HandleRequest();
			}
		}
		else
		{
			PS_LOG( "Session::HandleRead error=" << error.value() );
			//HandleError( error );
		}
	}

	void HandleRequest()
	{
	    job_.ParseRequest( request_ );

		boost::scoped_ptr< ScriptExec > scriptExec(
		    execCreator_.Create( job_.GetScriptLanguage() ) );
		scriptExec->Execute( &job_ );

		request_.Reset();
		Start();

		WriteResponse();
	}

	void WriteResponse()
	{
	    job_.GetResponse( response_ );

		boost::asio::async_write( socket_,
								boost::asio::buffer( response_ ),
	   							boost::bind( &Session::HandleWrite, shared_from_this(),
											 boost::asio::placeholders::error,
											 boost::asio::placeholders::bytes_transferred ) );
	}

	void HandleWrite( const boost::system::error_code& error, size_t bytes_transferred )
	{
		if ( error )
		{
			PS_LOG( "Session::HandleWrite error=" << error.value() );
		}
	}

protected:
	tcp::socket socket_;
	BufferType buffer_;
	Request< BufferType > request_;
    Job job_;
	ExecCreator execCreator_;
	std::string response_;
};


class ConnectionAcceptor
{
	typedef boost::shared_ptr< Session > session_ptr;

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
			acceptor_.bind( tcp::endpoint( tcp::v4(), port ) );
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
		session_ptr session( new Session( io_service_ ) );
		acceptor_.async_accept( session->GetSocket(),
								boost::bind( &ConnectionAcceptor::HandleAccept, this,
											session, boost::asio::placeholders::error ) );
	}

	void HandleAccept( session_ptr session, const boost::system::error_code &error )
	{
		if ( !error )
		{
			cout << "connection accepted..." << endl;
			io_service_.post( boost::bind( &Session::Start, session ) );
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

void SigHandler( int s )
{
	if ( s == SIGTERM )
	{
		exit( 0 );
	}

	if ( s == SIGCHLD )
	{
		// On Linux, multiple children terminating will be compressed into a single SIGCHLD
		while( 1 )
		{
			int status;
			pid_t pid = waitpid( -1, &status, WNOHANG );
			if ( pid <= 0 )
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

	sigaction( SIGTERM, &sigHandler, 0 );
	sigaction( SIGCHLD, &sigHandler, 0 );
	sigaction( SIGHUP, &sigHandler, 0 );
}

void SetupPyExecIPC()
{
    namespace ipc = boost::interprocess; 

	try
	{
        python_server::sharedMemPool = new ipc::shared_memory_object( ipc::open_only, python_server::SHMEM_NAME, ipc::read_only );
        python_server::mappedRegion = new ipc::mapped_region( *python_server::sharedMemPool, ipc::read_only );

        // crutch: get shared memory file path 
		char line[256] = { '\0' };
		std::ostringstream command;
		command << "lsof -Fn -p" << getpid() << "|grep " << python_server::SHMEM_NAME;
		FILE *cmd = popen( command.str().c_str(), "r" );
		fgets( line, sizeof(line), cmd );
		pclose( cmd );

		if ( !strlen( line ) )
		{
		    PS_LOG( "SetupPyExecIPC: error shared memory file not found");
			exit( 1 );
		}

        string path( line );
        size_t pos = path.find_first_of( '/' );
        size_t end = path.find_first_of( '\n' ) - 1;
        python_server::shmemPath = path.substr( pos, end );
	}
	catch( std::exception &e )
	{
		PS_LOG( "SetupPyExecIPC failed: " << e.what() );
		exit( 1 );
	}
}

void Impersonate()
{
	if ( python_server::uid )
	{
		int ret = setuid( python_server::uid );
		if ( ret < 0 )
		{
			PS_LOG( "impersonate uid=" << python_server::uid << " failed : " << strerror(errno) );
			exit( 1 );
		}

		PS_LOG( "successfully impersonated, uid=" << python_server::uid );
	}
}

void AtExit()
{
	if ( python_server::isFork )
		return;

	// cleanup threads
	python_server::ThreadInfo::iterator it;
	for( it = python_server::threadInfo.begin();
		 it != python_server::threadInfo.end();
	   ++it )
	{
		python_server::ThreadParams &threadParams = it->second;

        if ( threadParams.fifofd != -1 )
            close( threadParams.fifofd );

        if ( !threadParams.fifoName.empty() )
            unlink( threadParams.fifoName.c_str() );
	}

	delete python_server::mappedRegion;
	python_server::mappedRegion = NULL;

	delete python_server::sharedMemPool;
	python_server::sharedMemPool = NULL;

	python_server::logger::ShutdownLogger();

	kill( getppid(), SIGTERM );
}

void OnThreadCreate( const boost::thread *thread )
{
	static int threadCnt = 0;

	python_server::ThreadParams threadParams;
    threadParams.fifofd = -1;

    std::stringstream ss;
    ss << python_server::FIFO_NAME << threadCnt;
    threadParams.fifoName = ss.str();

	unlink( threadParams.fifoName.c_str() );

    int ret = mkfifo( threadParams.fifoName.c_str(), S_IRUSR | S_IWUSR );
    if ( !ret )
    {
        if ( python_server::uid )
        {
            ret = chown( threadParams.fifoName.c_str(), python_server::uid, -1 );
            if ( ret == -1 )
                PS_LOG( "OnThreadCreate: chown failed " << strerror(errno) );
        }

        threadParams.fifofd = open( threadParams.fifoName.c_str(), O_RDWR | O_NONBLOCK );
        if ( threadParams.fifofd == -1 )
        {
            PS_LOG( "open fifo " << threadParams.fifoName << " failed: " << strerror(errno) );
        }
    }
    else
    {
        PS_LOG( "OnThreadCreate: mkfifo failed " << strerror(errno) );
        threadParams.fifoName.clear();
    }
	++threadCnt;

	python_server::threadInfo[ thread->get_id() ] = threadParams;
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
	atexit( AtExit );

	try
	{
		// initialization
		python_server::isDaemon = false;
		python_server::isFork = false;
		python_server::uid = 0;

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
			python_server::isDaemon = true;
		}

		python_server::logger::InitLogger( python_server::isDaemon, "PyExec" );

		if ( vm.count( "u" ) )
		{
			python_server::uid = vm[ "u" ].as<uid_t>();
		}

		if ( vm.count( "num_thread" ) )
		{
			python_server::numThread = vm[ "num_thread" ].as<unsigned int>();
		}

		if ( vm.count( "exe_dir" ) )
		{
			python_server::exeDir = vm[ "exe_dir" ].as<std::string>();
            python_server::nodeScriptPath = python_server::exeDir + '/';
		}
        python_server::nodeScriptPath += python_server::NODE_SCRIPT_NAME;

        python_server::Config::Instance().ParseConfig( python_server::exeDir.c_str() );

	    SetupPyExecIPC();
		
		// start accepting connections
		boost::asio::io_service io_service;

		python_server::ConnectionAcceptor acceptor( io_service, python_server::DEFAULT_PYEXEC_PORT );

		// create thread pool
		boost::thread_group worker_threads;
		for( unsigned int i = 0; i < python_server::numThread; ++i )
		{
			boost::thread *thread = worker_threads.create_thread(
				boost::bind( &ThreadFun, &io_service )
			);
			OnThreadCreate( thread );
		}

		// signal parent process to say that PyExec has been initialized
		kill( getppid(), SIGUSR1 );

		Impersonate();

		if ( !python_server::isDaemon )
		{
			sigset_t waitset;
			int sig;
			sigemptyset( &waitset );
			sigaddset( &waitset, SIGTERM );
			sigwait( &waitset, &sig );
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

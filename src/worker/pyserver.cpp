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

#include <cerrno>
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
#include "request.h"
#include "common.h"
#include "common/helper.h"
#include "common/log.h"
#include "common/config.h"
#include "common/pidfile.h"


using namespace std;
using boost::asio::ip::tcp;

namespace python_server {

bool isDaemon;
uid_t uid;
unsigned int numThread;
pid_t pyexecPid;
string exeDir;

boost::interprocess::shared_memory_object *sharedMemPool;
boost::interprocess::mapped_region *mappedRegion;

struct ThreadComm
{
	int shmemBlockId;
	char *shmemAddr;
	tcp::socket *socket;
};

typedef std::map< boost::thread::id, ThreadComm > CommParams;
CommParams commParams;

Semaphore *taskSem;


class IActionStrategy
{
public:
	virtual void HandleRequest( const std::string &requestStr ) = 0;
	virtual const std::string &GetResponse() = 0;
	virtual void OnError( int err ) = 0;
	virtual ~IActionStrategy() {}
};

class SendToPyExec : public IActionStrategy
{
public:
	virtual void HandleRequest( const std::string &requestStr )
	{
		ThreadComm &threadComm = commParams[ boost::this_thread::get_id() ];
		memcpy( threadComm.shmemAddr, requestStr.c_str(), requestStr.size() );
		char *shmemRequestEnd = threadComm.shmemAddr + requestStr.size();
		*shmemRequestEnd = '\0';
	}

	virtual const std::string &GetResponse()
	{
		std::stringstream ss;

		// TODO: full error code description
		ptree_.put( "response", errCode_ ? "FAILED" : "OK" );

		boost::property_tree::write_json( ss, ptree_, false );
		response_ = ss.str();
		return response_;
	}

	virtual void OnError( int err )
	{
		errCode_ = err;
	}

private:
	boost::property_tree::ptree ptree_;
	std::string response_;
	int errCode_;
};

template< typename ActionPolicy >
class Action : private ActionPolicy
{
public:
	template< typename T >
	void HandleRequest( Request<T> &request )
	{
		const std::string &requestStr = request.GetRequestString();
		ActionPolicy::HandleRequest( requestStr );
	}

	virtual const std::string &GetResponse()
	{
		return ActionPolicy::GetResponse();
	}

	virtual void OnError( int err )
	{
		ActionPolicy::OnError( err );
	}
};

class PyExecConnection : public boost::enable_shared_from_this< PyExecConnection >
{
	typedef boost::array< char, 1024 > BufferType;

public:
	typedef boost::shared_ptr< PyExecConnection > connection_ptr;

public:
	PyExecConnection( boost::asio::io_service &io_service )
	{
		socket_ = commParams[ boost::this_thread::get_id() ].socket;
		memset( buffer_.c_array(), 0, buffer_.size() );
	}

	template< typename T >
	int Send( const Request< T > &request )
	{
		int ret = 0;

        execCompleted_ = false;

		try
		{
			std::stringstream ss, ss2;

			ThreadComm &threadComm = commParams[ boost::this_thread::get_id() ];
			ptree_.put( "id", threadComm.shmemBlockId );
			ptree_.put( "len", request.GetRequestLength() );

			boost::property_tree::write_json( ss, ptree_, false );

			ss2 << ss.str().size() << '\n' << ss.str();

			socket_->async_read_some( boost::asio::buffer( buffer_ ),
									 boost::bind( &PyExecConnection::HandleRead, shared_from_this(),
												  boost::asio::placeholders::error,
												  boost::asio::placeholders::bytes_transferred ) );

			boost::asio::async_write( *socket_,
									  boost::asio::buffer( ss2.str() ),
									  boost::bind( &PyExecConnection::HandleWrite, shared_from_this(),
												   boost::asio::placeholders::error,
												   boost::asio::placeholders::bytes_transferred ) );

			boost::unique_lock< boost::mutex > lock( responseMut_ );
            while( !execCompleted_ )
            {
                const boost::system_time timeout = boost::get_system_time() + boost::posix_time::milliseconds( RESPONSE_TIMEOUT );
                if ( responseCond_.timed_wait( lock, timeout ) )
                {
                    ret = errCode_;
                    break;
                }
                else
                {
                    PS_LOG( "PyExecConnection::Send(): timed out" );
                    ret = -1;
                    //break;
                }
            }
		}
		catch( boost::system::system_error &e )
		{
			PS_LOG( "PyExecConnection::Send() failed: " << e.what() );
			ret = -1;
		}

		return ret;
	}

	virtual void HandleWrite( const boost::system::error_code& error, size_t bytes_transferred )
	{
		if ( error )
		{
			PS_LOG( "PyExecConnection::HandleWrite error=" << error.value() );
		}
	}

	virtual void HandleRead( const boost::system::error_code& error, size_t bytes_transferred )
	{
		if ( error )
		{
			PS_LOG( "PyExecConnection::HandleRead error=" << error.value() );
		}
		else
		{
			std::stringstream ss;
			ptree_.clear();
			ss << buffer_.c_array();

		    boost::property_tree::read_json( ss, ptree_ );
			errCode_ = ptree_.get<int>( "err" );
		}

        execCompleted_ = true;
		boost::unique_lock< boost::mutex > lock( responseMut_ );
		responseCond_.notify_all();
	}

protected:
	boost::property_tree::ptree ptree_;
	tcp::socket *socket_;
	BufferType buffer_;
    bool execCompleted_; // true, if pyexec completed script execution
	boost::condition_variable responseCond_;
	boost::mutex responseMut_;
	int errCode_;
	const static int RESPONSE_TIMEOUT = 60 * 1000; // 60 sec
};

class Session : public boost::enable_shared_from_this< Session >
{
public:
	typedef boost::array< char, 32 * 1024 > BufferType;

public:
	Session( boost::asio::io_service &io_service )
	: socket_( io_service ),
	 io_service_( io_service )
	{
	}

	virtual ~Session()
	{
		taskSem->Notify();
		cout << "S: ~Session()" << endl;
	}

	virtual void Start()
	{
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
	virtual void FirstRead( const boost::system::error_code& error, size_t bytes_transferred )
	{
		if ( !error )
		{
			int ret = request_.OnFirstRead( buffer_, bytes_transferred );
			if ( ret < 0 )
			{
				action_.OnError( ret );
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

	virtual void HandleRead( const boost::system::error_code& error, size_t bytes_transferred )
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

	virtual void HandleRequest()
	{
		action_.HandleRequest( request_ );
		PyExecConnection::connection_ptr pyExecConnection( new PyExecConnection( io_service_ ) );
		int ret = pyExecConnection->Send( request_ );
		action_.OnError( ret );
		WriteResponse();
	}

	virtual void WriteResponse()
	{
	    response_ = action_.GetResponse();

		boost::asio::async_write( socket_,
								boost::asio::buffer( response_ ),
	   							boost::bind( &Session::HandleWrite, shared_from_this(),
								boost::asio::placeholders::error ) );
	}

	virtual void HandleWrite( const boost::system::error_code& error )
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
	Action< SendToPyExec > action_;
	std::string response_;
	boost::asio::io_service &io_service_;
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
			taskSem->Wait();
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

int StartAsDaemon()
{
	pid_t parpid, sid;

	// Fork the process and have the parent exit. If the process was started
	// from a shell, this returns control to the user. Forking a new process is
	// also a prerequisite for the subsequent call to setsid().
	parpid = fork();
	if ( parpid < 0 )
	{
		cout << "StartAsDaemon: fork() failed: " << strerror(errno) << endl;
		exit( parpid );
	}
	else
	if ( parpid > 0 )
	{
		exit( 0 );
	}

	// Make the process a new session leader. This detaches it from the
	// terminal.
	sid = setsid();
	if ( sid < 0 )
	{
		cout << "StartAsDaemon: setsid() failed: " << strerror(errno) << endl;
		exit( 1 );
	}

	// A process inherits its working directory from its parent. This could be
	// on a mounted filesystem, which means that the running daemon would
	// prevent this filesystem from being unmounted. Changing to the root
	// directory avoids this problem.
	chdir("/");

	// The file mode creation mask is also inherited from the parent process.
	// We don't want to restrict the permissions on files created by the
	// daemon, so the mask is cleared.
	umask(0);

	close(STDIN_FILENO);
	close(STDOUT_FILENO);
	close(STDERR_FILENO);

	return sid;
}

int StopDaemon()
{
	char line[256] = { '\0' };

	std::ostringstream command;
	command << "pidof -s -o " << getpid() << " pyserver";

	FILE *cmd = popen( command.str().c_str(), "r" );
	fgets( line, sizeof(line), cmd );
	pclose( cmd );

	if ( !strlen( line ) )
	{
		std::cout << "can't get pid of pyserver: " << strerror(errno) << std::endl;
		exit( 1 );
	}

	pid_t pid = strtoul( line, NULL, 10 );

	std::cout << "sending SIGTERM to " << pid << std::endl;
	return kill( pid, SIGTERM );
}

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
	if ( s == SIGTERM )
	{
		exit( 0 );
	}

	if ( s == SIGCHLD )
	{
	    int status;
		pid_t pid = waitpid( python_server::pyexecPid, &status, WNOHANG );
		// if ( pid != python_server::pyexecPid )
		// {
		// 	PS_LOG( "SigHandler: waitpid() failed, pid= " << python_server::pyexecPid << ", err= " << strerror(errno) );
		// }
		// else
		// {
		// 	PS_LOG( "PyExec proccess stopped (" << status << ")" );
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

	sigaction( SIGTERM, &sigHandler, 0 );
	sigaction( SIGUSR1, &sigHandler, 0 );
	sigaction( SIGCHLD, &sigHandler, 0 );
	sigaction( SIGHUP, &sigHandler, 0 );
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
						 ( boost::lexical_cast<std::string>( python_server::numThread ) ).c_str(),
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
		python_server::pyexecPid = pid;

		// wait while PyExec completes initialization
		sigset_t waitset;
		siginfo_t info;
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

		size_t shmemSize = python_server::numThread * python_server::SHMEM_BLOCK_SIZE;
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

void OnThreadCreate( const boost::thread *thread, boost::asio::io_service *io_service )
{
	static int commCnt = 0;

	// init shmem block associated with created thread
	python_server::ThreadComm threadComm;
	threadComm.shmemBlockId = commCnt;
	threadComm.shmemAddr = (char*)python_server::mappedRegion->get_address() + commCnt * python_server::SHMEM_BLOCK_SIZE;
	memset( threadComm.shmemAddr, 0, python_server::SHMEM_BLOCK_SIZE );

	boost::system::error_code ec;

	// open socket to pyexec
	tcp::resolver resolver( *io_service );
	tcp::resolver::query query( tcp::v4(), "localhost", boost::lexical_cast<std::string>( python_server::DEFAULT_PYEXEC_PORT ) );
	tcp::resolver::iterator iterator = resolver.resolve( query );

	threadComm.socket = new tcp::socket( *io_service );
	threadComm.socket->connect( *iterator, ec );
	if ( ec.value() )
	{
		PS_LOG( "OnThreadCreate: socket_.connect() failed " << ec.value() );
		exit( 1 );
	}

	++commCnt;

	python_server::commParams[ thread->get_id() ] = threadComm;
}

void CleanupThreads()
{
	python_server::CommParams::iterator it;
	for( it = python_server::commParams.begin();
		 it != python_server::commParams.end();
	   ++it )
	{
		python_server::ThreadComm &threadComm = it->second;

		if ( threadComm.socket )
		{
			delete threadComm.socket;
			threadComm.socket = NULL;
		}
	}
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

// TODO: error code description
// TODO: read directly to shmem, avoiding memory copying
int main( int argc, char* argv[], char **envp )
{
	try
	{
		// initialization
	    python_server::numThread = 2 * boost::thread::hardware_concurrency();
		python_server::isDaemon = false;
		python_server::uid = 0;

        python_server::exeDir = boost::filesystem::system_complete( argv[0] ).branch_path().string();

		// parse input command line options
		namespace po = boost::program_options;
		
		po::options_description descr;

		descr.add_options()
			("help", "Print help")
			("num_thread", po::value<unsigned int>(), "Thread pool size")
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
			return StopDaemon();
		}

		if ( vm.count( "u" ) )
		{
			python_server::uid = vm[ "u" ].as<uid_t>();
		}
		VerifyCommandlineParams();

		if ( vm.count( "d" ) )
		{
			StartAsDaemon();
			python_server::isDaemon = true;
		}

		if ( vm.count( "num_thread" ) )
		{
			python_server::numThread = vm[ "num_thread" ].as<unsigned int>();
		}
		// accept thread & additional worker thread for async reading results from pyexec
		python_server::numThread += 2;

		python_server::logger::InitLogger( python_server::isDaemon, "PythonServer" );

        python_server::Config::Instance().ParseConfig( python_server::exeDir.c_str() );

        string pidfilePath = python_server::Config::Instance().Get<string>( "pidfile" );
        if ( pidfilePath[0] != '/' )
        {
            pidfilePath = python_server::exeDir + '/' + pidfilePath;
        }
        python_server::Pidfile pidfile( pidfilePath.c_str() );

		SetupSignalHandlers();
		atexit( AtExit );

		SetupPyExecIPC();
		RunPyExecProcess();

		// start accepting client connections
		boost::asio::io_service io_service;

		python_server::taskSem = new python_server::Semaphore( python_server::numThread - 2 );

		python_server::ConnectionAcceptor acceptor( io_service, python_server::DEFAULT_PORT );

		// create thread pool
		boost::thread_group worker_threads;
		for( unsigned int i = 0; i < python_server::numThread; ++i )
		{
			boost::thread *thread = worker_threads.create_thread(
				boost::bind( &ThreadFun, &io_service )
			);
			OnThreadCreate( thread, &io_service );
		}

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
			sigwait( &waitset, &sig );
		}

		io_service.stop();

		python_server::taskSem->Notify();

		worker_threads.join_all();

		CleanupThreads();
	}
	catch( std::exception &e )
	{
		cout << "Exception: " << e.what() << endl;
		PS_LOG( "Exception: " << e.what() );
	}

	PS_LOG( "stopped" );

	return 0;
}

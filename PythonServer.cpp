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
#include <unistd.h>
#include <csignal>
#include <cstdlib>
#include "Common.h"
#include "Log.h"


using namespace std;
using boost::asio::ip::tcp;

namespace python_server {

bool isDaemon;
bool forkMode;
uid_t uid;
unsigned int numThread;
pid_t pyexecPid;
char exeDir[256];

boost::interprocess::shared_memory_object *sharedMemPool;
boost::interprocess::mapped_region *mappedRegion;

struct ThreadComm
{
	int shmemBlock;
	char *shmemAddr;
};

typedef std::map< boost::thread::id, ThreadComm > CommParams;
CommParams commParams;


template< typename BufferT >
class Request
{
public:
	Request()
	: requestLength_( 0 ),
	 bytesRead_( 0 ),
	 headerOffset_( 0 )
	{
	}

	void OnRead( BufferT &buf, size_t bytes_transferred  )
	{
		std::copy( buf.begin() + headerOffset_, buf.begin() + bytes_transferred, back_inserter( request_ ) );

		bytesRead_ += bytes_transferred - headerOffset_;
	}

	int OnFirstRead( BufferT &buf, size_t bytes_transferred  )
	{
		headerOffset_ = ParseRequestHeader( buf, bytes_transferred );
		return CheckHeader();
	}

	bool IsReadCompleted() const
	{
		return bytesRead_ >= requestLength_;
	}

	const std::string &GetRequestString() const
	{
		return request_;
	}

	int GetRequestLength() const
	{
		return requestLength_;
	}

private:
	int ParseRequestHeader( BufferT &buf, size_t bytes_transferred  )
	{
		int offset = 0;
		std::string length;

		typename BufferT::iterator it = std::find( buf.begin(), buf.begin() + bytes_transferred, '\n' );
		if ( it != buf.end() )
		{
			offset = (int)std::distance( buf.begin(), it );
			std::copy( buf.begin(), buf.begin() + offset, back_inserter( length ) );

			try
			{
				requestLength_ = boost::lexical_cast<unsigned int>( length );
			}
			catch( boost::bad_lexical_cast &e )
			{
				PS_LOG( "Reading request length failed: " << e.what() );
			}
		}
		else
		{
			PS_LOG( "Reading request length failed: new line not found" );
		}

		return offset;
	}

	int CheckHeader()
	{
		// TODO: Error codes
		if ( headerOffset_ > maxScriptSize )
			return -1;

		return 0;
	}

private:
	std::string request_;
	int	requestLength_;
	int bytesRead_;
	unsigned int headerOffset_;
};

class IActionStrategy
{
public:
	virtual void HandleRequest( const std::string &requestStr ) = 0;
	virtual const std::string &GetResponse() = 0;
	virtual void OnError( int err ) = 0;
};

class SendToPyExec : public IActionStrategy
{
public:
	virtual void HandleRequest( const std::string &requestStr )
	{
		int ret = 0;

		ThreadComm &threadComm = commParams[ boost::this_thread::get_id() ];
		memcpy( threadComm.shmemAddr, requestStr.c_str(), requestStr.size() );
		char *addr = threadComm.shmemAddr + requestStr.size();
		*addr = '\0';

		errCode_ = ret;
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
	: socket_( io_service )
	{
		boost::system::error_code ec;

		tcp::resolver resolver( io_service );
		tcp::resolver::query query( tcp::v4(), "localhost", boost::lexical_cast<std::string>( python_server::defaultPyExecPort ) );
		tcp::resolver::iterator iterator = resolver.resolve( query );
		socket_.connect( *iterator, ec );
		if ( ec.value() )
		{
			PS_LOG( "PyExecConnection: socket_.connect() failed " << ec.value() );
		}

		memset( buffer_.c_array(), 0, buffer_.size() );
	}

	template< typename T >
	int Send( const Request< T > &request )
	{
		int ret = 0;

		try
		{
			std::stringstream ss, ss2;

			ThreadComm &threadComm = commParams[ boost::this_thread::get_id() ];
			ptree_.put( "id", threadComm.shmemBlock );

			boost::property_tree::write_json( ss, ptree_, false );
			
			ss2 << ss.str().size() << '\n' << ss.str();

			boost::asio::async_write( socket_,
									  boost::asio::buffer( ss2.str() ),
									  boost::bind( &PyExecConnection::HandleWrite, shared_from_this(),
												   boost::asio::placeholders::error,
												   boost::asio::placeholders::bytes_transferred ) );

			const boost::system_time timeout = boost::get_system_time() + boost::posix_time::milliseconds( responseTimeout );

			boost::unique_lock< boost::mutex > lock( mut );
		    if ( cond.timed_wait( lock, timeout ) )
			{
				ret = errCode_;
			}
			else
			{
				ret = -1;
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
		else
		{
			socket_.async_read_some( boost::asio::buffer( buffer_ ),
									 boost::bind( &PyExecConnection::HandleRead, shared_from_this(),
												  boost::asio::placeholders::error,
												  boost::asio::placeholders::bytes_transferred ) );
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

		boost::unique_lock< boost::mutex > lock( mut );
		cond.notify_all();
	}

protected:
	boost::property_tree::ptree ptree_;
	tcp::socket socket_;
	BufferType buffer_;
	boost::condition_variable cond;
	boost::mutex mut;
	int errCode_;
	const int responseTimeout = 10 * 1000; // 10 sec
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
		const std::string &response = action_.GetResponse();

		boost::asio::async_write( socket_,
								boost::asio::buffer( response ),
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
			boost::asio::ip::tcp::endpoint endpoint( tcp::v4(), port );
			acceptor_.open( endpoint.protocol() );
			acceptor_.set_option( boost::asio::ip::tcp::acceptor::reuse_address( true ) );
			acceptor_.bind( tcp::endpoint( tcp::v4(), port ) );
			acceptor_.listen();
		}
		catch( std::exception &e )
		{
			PS_LOG( "ConnectionAcceptor: " << e.what() );
		}

		StartAccept();
	}

	void StartAccept()
	{
		session_ptr session( new Session( io_service_ ) );
		acceptor_.async_accept( session->GetSocket(),
								boost::bind( &ConnectionAcceptor::HandleAccept, this,
											session, boost::asio::placeholders::error ) );
	}

private:
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

int StartAsDaemon()
{
//#ifdef _BSD_SOURCE || (_XOPEN_SOURCE && _XOPEN_SOURCE < 500)
	//return daemon(1, 1);
//#else
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
//#endif
}

int StopDaemon()
{
	char line[256] = { '\0' };

	std::ostringstream command;
	command << "pidof -s -o " << getpid() << " PythonServer";

	FILE *cmd = popen( command.str().c_str(), "r" );
	fgets( line, sizeof(line), cmd );
	pid_t pid = strtoul( line, NULL, 10 );
	pclose( cmd );

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
		exePath += "/PyExec";

		int ret = execl( exePath.c_str(), "./PyExec", "--num_thread",
						 ( boost::lexical_cast<std::string>( python_server::numThread ) ).c_str(),
						 python_server::isDaemon ? "--d" : " ",
						 python_server::uid != 0 ? "--u" : " ",
						 python_server::uid != 0 ? ( boost::lexical_cast<std::string>( python_server::uid ) ).c_str() : " ",
						 python_server::forkMode ? "--f" : " ",
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

	ipc::shared_memory_object::remove( python_server::shmemName );

	try
	{
		python_server::sharedMemPool = new ipc::shared_memory_object( ipc::create_only, python_server::shmemName, ipc::read_write );

		size_t shmemSize = python_server::numThread * python_server::shmemBlockSize;
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
	ipc::shared_memory_object::remove( python_server::shmemName );

	if ( python_server::mappedRegion )
	{
		delete python_server::mappedRegion;
		python_server::mappedRegion = NULL;
	}

	if ( python_server::sharedMemPool )
	{
		delete python_server::sharedMemPool;
		python_server::sharedMemPool = NULL;
	}

	python_server::logger::ShutdownLogger();
}

void OnThreadCreate( const boost::thread *thread )
{
	static int commCnt = 0;

	python_server::ThreadComm threadComm;
	threadComm.shmemBlock = commCnt;
	threadComm.shmemAddr = (char*)python_server::mappedRegion->get_address() + commCnt * python_server::shmemBlockSize;
	memset( threadComm.shmemAddr, 0, python_server::shmemBlockSize );
	++commCnt;

	python_server::commParams[ thread->get_id() ] = threadComm;
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
// TODO: auto dependency generation in makefile
// TODO: read directly to shmem, avoiding memory copying
int main( int argc, char* argv[], char **envp )
{
	getcwd( python_server::exeDir, sizeof( python_server::exeDir ) );

	try
	{
		// initialization
	    python_server::numThread = 2 * boost::thread::hardware_concurrency();
		python_server::isDaemon = false;
		python_server::forkMode = false;
		python_server::uid = 0;

		// parse input command line options
		namespace po = boost::program_options;
		
		po::options_description descr;

		descr.add_options()
			("help", "Print help")
			("num_thread", po::value<unsigned int>(), "Thread pool size")
			("d", "Run as a daemon")
			("stop", "Stop daemon")
			("u", po::value<uid_t>(), "Start as a specific non-root user")
			("f", "Create process for each request");
		
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

		if ( vm.count( "f" ) )
		{
			python_server::forkMode = true;
		}

		if ( vm.count( "num_thread" ) )
		{
			python_server::numThread = vm[ "num_thread" ].as<unsigned int>();
		}

		python_server::logger::InitLogger( python_server::isDaemon, "PythonServer" );

		SetupSignalHandlers();
		atexit( AtExit );

		SetupPyExecIPC();
		RunPyExecProcess();
		
		// start accepting client connections
		boost::asio::io_service io_service;

		python_server::ConnectionAcceptor acceptor( io_service, python_server::defaultPort );

		// create thread pool
		boost::thread_group worker_threads;
		for( unsigned int i = 0; i < python_server::numThread; ++i )
		{
			boost::thread *thread = worker_threads.create_thread(
				boost::bind( &ThreadFun, &io_service )
			);
			OnThreadCreate( thread );
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
		worker_threads.join_all();
	}
	catch( std::exception &e )
	{
		cout << e.what() << endl;
		PS_LOG( e.what() );
	}

	PS_LOG( "stopped" );

	return 0;
}

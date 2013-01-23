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
#include <Python.h>
#include <unistd.h>
#include <csignal>
#include <sys/wait.h>
#include "Common.h"
#include "Log.h"


using namespace std;
using boost::asio::ip::tcp;

namespace python_server {

bool isDaemon;
bool forkMode;
bool isFork;
uid_t uid;
unsigned int numThread;

boost::interprocess::shared_memory_object *sharedMemPool;
boost::interprocess::mapped_region *mappedRegion;

struct ThreadParams
{
	boost::condition_variable *cond;
	boost::mutex *mut;
	pid_t childPid;
	int errCode;
};

typedef std::map< boost::thread::id, ThreadParams > ThreadInfo;
ThreadInfo threadInfo;

boost::mutex waitTableMut;
std::map< pid_t, boost::thread::id > waitTable;


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
};

class ExecutePython : public IActionStrategy
{
public:
	virtual void HandleRequest( const std::string &requestStr )
	{
		//using namespace boost::python;
		//exec( str( requestStr ) );

		std::stringstream ss;
		ss << requestStr;

		boost::property_tree::read_json( ss, ptree_ );
		int id = ptree_.get<int>( "id" );

		pid_t pid = 0;
		if ( python_server::forkMode )
		{
			pid = DoFork();
			if ( pid > 0 )
				return;
		}

		size_t offset = id * python_server::shmemBlockSize;
		char *addr = (char*)python_server::mappedRegion->get_address() + offset;

		//PyCompilerFlags cf;
		//cf.cf_flags = 0; // TODO: ignore os._exit(), sys.exit(), thread.exit(), etc.
	    errCode_ = PyRun_SimpleStringFlags( addr, NULL );

		if ( python_server::forkMode && pid == 0 )
		{
			exit( errCode_ );
		}
	}

	virtual pid_t DoFork()
	{
		pid_t pid;

		pid = fork();
		if ( pid > 0 )
		{
			waitTableMut.lock();
			waitTable[ pid ] = boost::this_thread::get_id();
			waitTableMut.unlock();

			//PS_LOG( "wait child " << pid );
			ThreadParams &threadParams = threadInfo[ boost::this_thread::get_id() ];
			boost::unique_lock< boost::mutex > lock( *threadParams.mut );
			threadParams.cond->wait( lock );
		    errCode_ = threadParams.errCode;
			//PS_LOG( "wait child done " << pid );
		}
		else
		if ( pid == 0 )
		{
			python_server::isFork = true;
		}
		else
		{
			PS_LOG( "DoFork: fork() failed " << strerror(errno) );
		}

		return pid;
	}

	virtual const std::string &GetResponse()
	{
		std::stringstream ss;

		// TODO: full error code description
		ptree_.put( "err", errCode_ );

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

class Session : public boost::enable_shared_from_this< Session >
{
	typedef boost::array< char, 8 * 1024 > BufferType;

public:
	Session( boost::asio::io_service &io_service )
	: socket_( io_service )
	{
	}

	virtual ~Session()
	{
		cout << "~Session()" << endl;
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
			//HandleError( error );
		}
	}

	virtual void HandleRequest()
	{
		action_.HandleRequest( request_ );
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
	}

protected:
	tcp::socket socket_;
	BufferType buffer_;
	Request< BufferType > request_;
	Action< ExecutePython > action_;
};


class ConnectionAcceptor
{
	typedef boost::shared_ptr< Session > session_ptr;

public:
	ConnectionAcceptor( boost::asio::io_service &io_service, unsigned short port )
	: io_service_( io_service ),
	  acceptor_( io_service, tcp::endpoint( tcp::v4(), port ) )
	{
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
			PS_LOG( error.message() );
			cout << error.message() << endl;
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

	if ( s == SIGCHLD && python_server::forkMode )
	{
		// On Linux, multiple children terminating will be compressed into a single SIGCHLD
		while( 1 )
		{
			int status;
			pid_t pid = waitpid( -1, &status, WNOHANG );
			if ( pid <= 0 )
				break;

			//PS_LOG( "SIGCHLD " << pid );
			python_server::waitTableMut.lock();
			boost::thread::id threadId = python_server::waitTable[ pid ];
			python_server::waitTableMut.unlock();

			python_server::ThreadParams &threadParams = python_server::threadInfo[ threadId ];
			boost::unique_lock< boost::mutex > lock( *threadParams.mut );
			threadParams.errCode = status;

			threadParams.cond->notify_all();
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
}

void SetupPyExecIPC()
{
	namespace ipc = boost::interprocess;

    python_server::sharedMemPool = new ipc::shared_memory_object( ipc::open_only, python_server::shmemName, ipc::read_only );
	python_server::mappedRegion = new ipc::mapped_region( *python_server::sharedMemPool, ipc::read_only );
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
	}
}

void AtExit()
{
	if ( python_server::isFork )
		return;

	kill( getppid(), SIGTERM );

	python_server::logger::ShutdownLogger();
}

void OnThreadCreate( const boost::thread *thread )
{
	static int threadCnt = 0;

	python_server::ThreadParams threadParams;
	memset( &threadParams, 0, sizeof( threadParams ) );
	if ( python_server::forkMode )
	{
		threadParams.cond = new boost::condition_variable();
		threadParams.mut = new boost::mutex();
	}
	++threadCnt;

	python_server::threadInfo[ thread->get_id() ] = threadParams;
}

} // anonymous namespace


int main( int argc, char* argv[], char **envp )
{
	SetupSignalHandlers();
	atexit( AtExit );

	Py_Initialize();

	try
	{
		// initialization
		python_server::isDaemon = false;
		python_server::forkMode = false;
		python_server::isFork = false;
		python_server::uid = 0;

		// parse input command line options
		namespace po = boost::program_options;
		
		po::options_description descr;

		descr.add_options()
			("num_thread", po::value<unsigned int>(), "Thread pool size")
			("d", "Run as a daemon")
			("u", po::value<uid_t>(), "Start as a specific non-root user")
			("f", "Create process for each request");
		
		po::variables_map vm;
		po::store( po::parse_command_line( argc, argv, descr ), vm );
		po::notify( vm );

		if ( vm.count( "u" ) )
		{
			python_server::uid = vm[ "u" ].as<uid_t>();
		}

		if ( vm.count( "d" ) )
		{
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

		python_server::logger::InitLogger( python_server::isDaemon, "PyExec" );

		SetupPyExecIPC();
		
		// start accepting connections
		boost::asio::io_service io_service;

		python_server::ConnectionAcceptor acceptor( io_service, python_server::defaultPyExecPort );

		// create thread pool
		boost::thread_group worker_threads;
		for( unsigned int i = 0; i < python_server::numThread; ++i )
		{
			boost::thread *thread = worker_threads.create_thread(
				boost::bind( &boost::asio::io_service::run, &io_service )
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
		cout << e.what() << endl;
		PS_LOG( e.what() );
	}

	Py_Finalize();

	PS_LOG( "stopped" );

	return 0;
}

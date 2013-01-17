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
#include <unistd.h>
#include <syslog.h>
#include <csignal>
#include <cstdlib>
#include "Common.h"


using namespace std;
using boost::asio::ip::tcp;

namespace python_server {

bool isDaemon;
uid_t uid;
gid_t gid;
unsigned int numThread;
pid_t pyexecPid;

struct CommPort
{
	boost::interprocess::shared_memory_object *shmem;
	//
};

typedef std::map< boost::thread::id, CommPort > ShmemPoolType;
ShmemPoolType sharedMemPool;


template< typename BufferT >
class Request
{
public:
	Request()
	: requestLength_( 0 ),
	 bytesRead_( 0 ),
	 firstRead_( true )
	{
	}

	int ReadMessageLength( BufferT &buf, size_t bytes_transferred  )
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
				requestLength_ = boost::lexical_cast<int>( length );
			}
			catch( boost::bad_lexical_cast &e )
			{
				cout << "Reading request length failed: " << e.what() << endl;
			}
		}
		else
		{
			cout << "Reading request length failed: new line not found" << endl;
		}

		return offset;
	}

	void OnRead( BufferT &buf, size_t bytes_transferred  )
	{
		int skip_offset = 0;

		if ( firstRead_ )
		{
			skip_offset = ReadMessageLength( buf, bytes_transferred );
			firstRead_ = false;
		}

		std::copy( buf.begin() + skip_offset, buf.begin() + bytes_transferred, back_inserter( request_ ) );

		bytesRead_ += bytes_transferred - skip_offset;
	}

	bool IsReadCompleted() const
	{
		return bytesRead_ >= requestLength_;
	}

	const std::string &GetRequestString() const
	{
		return request_;
	}

private:
	std::string request_;
	int	requestLength_;
	int bytesRead_;
	bool firstRead_;
};

class IActionStrategy
{
public:
	virtual void HandleRequest( const std::string &requestStr ) = 0;
	virtual const std::string &GetResponse() const = 0;
};

class SendToPyExec : public IActionStrategy
{
public:
	virtual void HandleRequest( const std::string &requestStr )
	{
		int ret = 0;
		ptree_.put( "response", ret ? "FAILED" : "OK" );
	}

	virtual const std::string &GetResponse() const
	{
		std::stringstream ss;
		boost::property_tree::write_json( ss, ptree_, false );
		response_ = ss.str();
		return response_;
	}

private:
	boost::property_tree::ptree ptree_;
	mutable std::string response_;
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

	virtual const std::string &GetResponse() const
	{
		return ActionPolicy::GetResponse();
	}
};

class Session : public boost::enable_shared_from_this< Session >
{
	typedef boost::array< char, 32 * 1024 > BufferType;

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
								 boost::bind( &Session::HandleRead, shared_from_this(),
											boost::asio::placeholders::error,
											boost::asio::placeholders::bytes_transferred ) );
	}

	tcp::socket &GetSocket()
	{
		return socket_;
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
				action_.HandleRequest( request_ );

				const std::string &response = action_.GetResponse();

				boost::asio::async_write( socket_,
										boost::asio::buffer( response ),
										boost::bind( &Session::HandleWrite, shared_from_this(),
										boost::asio::placeholders::error ) );
			}
		}
		else
		{
			//HandleError( error );
		}
	}

	virtual void HandleWrite( const boost::system::error_code& error )
	{
	}

protected:
	tcp::socket socket_;
	BufferType buffer_;
	Request< BufferType > request_;
	Action< SendToPyExec > action_;
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
			cout << error.message() << endl;
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

		// extract gid
		line[0] = '\0';
		command.str("");
		command.clear();

		command << "getent passwd " << python_server::uid << "|cut -d: -f4";

	    cmd = popen( command.str().c_str(), "r" );
		fgets( line, sizeof(line), cmd );
		pclose( cmd );

		if ( !strlen( line ) )
		{
			std::cout << "Could not extract gid value" << std::endl;
			exit( 1 );
		}

		python_server::gid = strtoul( line, NULL, 10 );
	}
	else
	{
		if ( getuid() == 0 )
		{
			std::cout << "Could not execute python code due to security issues" << std::endl <<
				"Please use --u command line parameter for using uid of non-privileged user" << std::endl;
			exit( 1 );
		}
		else
		{
			python_server::uid = getuid();
		}
	}
}

void SigTermHandler( int s )
{
	//exit(0);
}

void SetupSignalHandlers()
{
	struct sigaction sigHandler;
	memset( &sigHandler, 0, sizeof( sigHandler ) );
	sigHandler.sa_handler = SigTermHandler;
	sigemptyset(&sigHandler.sa_mask);
	sigHandler.sa_flags = 0;

	sigaction( SIGTERM, &sigHandler, 0 );
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
		cout << "RunPyExecProcess: fork() failed: " << strerror(errno) << endl;
		exit( pid );
	}
	else
	if ( pid == 0 )
	{
		execl( "PyExec", "", NULL );
	}
	else
	if ( pid > 0 )
	{
		python_server::pyexecPid = pid;
	}
}

void SetupPyExecIPC( const boost::thread *thread )
{
	namespace ipc = boost::interprocess;

	static int commCnt = 0;
	std::string shmemName( "PyExec" + boost::lexical_cast<std::string>( commCnt++ ) );

	ipc::shared_memory_object::remove( shmemName.c_str() );

	python_server::CommPort port;
    port.shmem = new ipc::shared_memory_object( ipc::create_only, shmemName.c_str(), ipc::read_write );

	python_server::sharedMemPool[ thread->get_id() ] = port;
}

void AtExit()
{
	namespace ipc = boost::interprocess;
	python_server::ShmemPoolType::iterator it;

	for( it = python_server::sharedMemPool.begin();
		 it != python_server::sharedMemPool.end();
	   ++it )
	{
		if ( it->second.shmem )
		{
			ipc::shared_memory_object::remove( it->second.shmem->get_name() );
			delete it->second.shmem;
			it->second.shmem = NULL;
		}
	}
}

} // anonymous namespace

// TODO: normal logging system
// TODO: auto dependency generation in makefile
int main( int argc, char* argv[], char **envp )
{
	SetupSignalHandlers();
	atexit( AtExit );

	try
	{
		// initialization
	    python_server::numThread = 2;
		python_server::isDaemon = false;

		// parse input command line options
		namespace po = boost::program_options;
		
		po::options_description descr;

		descr.add_options()
			("help", "Print help")
			("num_thread", po::value<unsigned int>(), "Thread pool size")
			("d", "Run as a daemon")
			("stop", "Stop daemon")
			("u", po::value<int>(), "Start as a specific non-root user");
		
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

		python_server::uid = 0;
		if ( vm.count( "u" ) )
		{
			python_server::uid = vm[ "u" ].as<int>();
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

		RunPyExecProcess();
		
		// start accepting client connections
		boost::asio::io_service io_service;

		python_server::ConnectionAcceptor acceptor( io_service, 5555 );

		// create thread pool
		boost::thread_group worker_threads;
		for( unsigned int i = 0; i < python_server::numThread; ++i )
		{
			boost::thread *thread = worker_threads.create_thread(
				boost::bind( &boost::asio::io_service::run, &io_service )
			);
			SetupPyExecIPC( thread );
		}

		// TODO: send signal to PyExec for starting

		if ( !python_server::isDaemon )
		{
			UserInteraction();
		}
		else
		{
			syslog( LOG_INFO | LOG_USER, "PythonServer daemon started" );

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
	}

	if ( python_server::isDaemon )
		syslog( LOG_INFO | LOG_USER, "PythonServer daemon stopped" );

	cout << "done..." << endl;
	return 0;
}

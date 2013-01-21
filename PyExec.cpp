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
#include <syslog.h>
#include <csignal>
#include "Common.h"


using namespace std;
using boost::asio::ip::tcp;

namespace python_server {

bool isDaemon;
uid_t uid;
unsigned int numThread;

boost::interprocess::shared_memory_object *sharedMemPool;
boost::interprocess::mapped_region *mappedRegion;


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
				cout << "Reading request length failed: " << e.what() << endl;
			}
		}
		else
		{
			cout << "Reading request length failed: new line not found" << endl;
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

		size_t offset = id * python_server::shmemBlockSize;
		char *addr = (char*)python_server::mappedRegion->get_address() + offset;

		PyCompilerFlags cf;
		cf.cf_flags = -1; // ignore os._exit()
	    errCode_ = PyRun_SimpleStringFlags( addr, &cf );
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
}

void SetupSignalHandlers()
{
	struct sigaction sigHandler;
	memset( &sigHandler, 0, sizeof( sigHandler ) );
	sigHandler.sa_handler = SigHandler;
	sigemptyset(&sigHandler.sa_mask);
	sigHandler.sa_flags = 0;

	sigaction( SIGTERM, &sigHandler, 0 );
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
			syslog( LOG_INFO | LOG_USER, "PyExec daemon impersonate uid=%d failed : %s", python_server::uid, strerror(errno) );
			exit( 1 );
		}
	}
}

void AtExit()
{
	kill( getppid(), SIGTERM );
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
		python_server::uid = 0;

		// parse input command line options
		namespace po = boost::program_options;
		
		po::options_description descr;

		descr.add_options()
			("num_thread", po::value<unsigned int>(), "Thread pool size")
			("d", "Run as a daemon")
			("u", po::value<uid_t>(), "Start as a specific non-root user");
		
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

		if ( vm.count( "num_thread" ) )
		{
			python_server::numThread = vm[ "num_thread" ].as<unsigned int>();
		}

		SetupPyExecIPC();
		
		// start accepting connections
		boost::asio::io_service io_service;

		python_server::ConnectionAcceptor acceptor( io_service, python_server::defaultPyExecPort );
		std::cout << python_server::numThread << std::cout;
		// create thread pool
		boost::thread_group worker_threads;
		for( unsigned int i = 0; i < python_server::numThread; ++i )
		{
			worker_threads.create_thread(
				boost::bind( &boost::asio::io_service::run, &io_service )
			);
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
			syslog( LOG_INFO | LOG_USER, "PyExec daemon started" );

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

	Py_Finalize();

	if ( python_server::isDaemon )
		syslog( LOG_INFO | LOG_USER, "PyExec daemon stopped" );

	return 0;
}

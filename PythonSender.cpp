#include <iostream>
#include <istream>
#include <ostream>
#include <fstream>
#include <string>
#include <boost/filesystem.hpp>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;


int main(int argc, char* argv[])
{
	try
	{
		if (argc != 4)
		{
			std::cout << "Usage: PythonSender <server> <port> <file_path>" << std::endl;
			return 1;
		}

		std::string filePath( argv[3] );

		// read file content
		if ( !boost::filesystem::exists( filePath ) )
		{
			std::cout << "File not found: " << filePath << std::endl;
			return 1;
		}

		  std::ifstream ifs( filePath.c_str(), std::ifstream::in );
		std::string content( ( std::istreambuf_iterator<char>( ifs ) ), std::istreambuf_iterator<char>() );

		// send file content to python server
		boost::asio::io_service io_service;

		tcp::resolver resolver( io_service );
		tcp::resolver::query query( argv[1], argv[2] );
		tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

		tcp::socket socket( io_service );
		boost::asio::connect( socket, endpoint_iterator );

		boost::asio::streambuf request;
		std::ostream request_stream(&request);
		request_stream << content.size() << std::endl;
		request_stream << content;

		boost::asio::write( socket, request );

		// read response from python server
		boost::asio::streambuf response;
		// boost::asio::read_until( socket, response, "\r\n\r\n" );
		try
		{
			boost::asio::read( socket, response );
		}
		catch( std::exception & ) {}

		std::istream response_stream( &response );
		std::string responseStr;
		response_stream >> responseStr;

		std::cout << responseStr << std::endl;
	}
	catch( std::exception &e )
	{
		std::cout << e.what() << std::endl;
	}

	std::cout << "done..." << std::endl;
	return 0;
}

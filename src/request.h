#include "log.h"
#include "common.h"

namespace python_server {

template< typename BufferT >
class Request
{
public:
	Request()
	: requestLength_( 0 ),
	 bytesRead_( 0 ),
	 headerOffset_( 0 ),
	 skipHeader_( true )
	{
	}

	void OnRead( BufferT &buf, size_t bytes_transferred )
	{
		unsigned int offset = skipHeader_ ? headerOffset_ : 0;

		std::copy( buf.begin() + offset, buf.begin() + bytes_transferred, back_inserter( request_ ) );

		bytesRead_ += bytes_transferred - offset;
		skipHeader_ = false;
	}

	int OnFirstRead( BufferT &buf, size_t bytes_transferred )
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

	unsigned int GetRequestLength() const
	{
		return requestLength_;
	}

	void Reset()
	{
		request_.clear();
		requestLength_ = bytesRead_ = headerOffset_ = 0;
		skipHeader_ = true;
	}

private:
	unsigned int ParseRequestHeader( BufferT &buf, size_t bytes_transferred )
	{
		unsigned int offset = 0;
		std::string length;

		typename BufferT::iterator it = std::find( buf.begin(), buf.begin() + bytes_transferred, '\n' );
		if ( it != buf.end() )
		{
			offset = (unsigned int)std::distance( buf.begin(), it );
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
		if ( requestLength_ > maxScriptSize )
			return -1;

		return 0;
	}

private:
	std::string request_;
	unsigned int requestLength_;
	unsigned int bytesRead_;
	unsigned int headerOffset_;
	bool skipHeader_;
};

} // namespace python_server

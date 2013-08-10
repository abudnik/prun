#ifndef __JOB_RESULT_H
#define __JOB_RESULT_H

#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include "common/observer.h"
#include "common/helper.h"
#include "common/request.h"
#include "worker.h"

using boost::asio::ip::tcp;

namespace master {

class ResultGetter : python_server::Observer
{
public:
    ResultGetter()
	: stopped_( false ), newJobAvailable_( false )
	{}

    virtual void Start() = 0;

    void Stop();

    void Run();

	virtual void OnGetJobResult( bool success, const Worker *worker );

private:
    virtual void NotifyObserver( int event );

	virtual void GetJobResult( const Worker *worker ) = 0;

private:
    bool stopped_;

    boost::mutex awakeMut_;
    boost::condition_variable awakeCond_;
    bool newJobAvailable_;
};

class GetterBoost : public boost::enable_shared_from_this< GetterBoost >
{
	typedef boost::array< char, 32 * 1024 > BufferType;

public:
	typedef boost::shared_ptr< GetterBoost > getter_ptr;

public:
	GetterBoost( boost::asio::io_service &io_service,
				 ResultGetter *getter, const Worker *worker )
	: io_service_( io_service ), socket_( io_service ),
	 response_( false ),
	 getter_( getter ), worker_( worker )
	{}

	void GetJobResult();

private:
	void HandleConnect( const boost::system::error_code &error );

    void MakeRequest();

    void HandleWrite( const boost::system::error_code &error, size_t bytes_transferred );

    void FirstRead( const boost::system::error_code &error, size_t bytes_transferred );

    void HandleRead( const boost::system::error_code &error, size_t bytes_transferred );

	void HandleResponse();

private:
    boost::asio::io_service &io_service_;
	tcp::socket socket_;
	BufferType buffer_;
	python_server::Request< BufferType > response_;
    std::string request_;
	ResultGetter *getter_;
	const Worker *worker_;
};

class ResultGetterBoost : public ResultGetter
{
public:
    ResultGetterBoost( boost::asio::io_service &io_service,
					   int maxSimultResultGetters )
    : io_service_( io_service ),
	 getJobsSem_( maxSimultResultGetters )
    {}

    virtual void Start();

private:
	virtual void GetJobResult( const Worker *worker );

    virtual void OnGetJobResult( bool success, const Worker *worker );

private:
    boost::asio::io_service &io_service_;
	python_server::Semaphore getJobsSem_;
};

} // namespace master

#endif

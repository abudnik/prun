#ifndef __COMMAND_SENDER_H
#define __COMMAND_SENDER_H

#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include "common/observer.h"
#include "common/helper.h"
#include "common/request.h"
#include "worker.h"

using boost::asio::ip::tcp;

namespace master {

class CommandSender : python_server::Observer
{
public:
    CommandSender()
    : stopped_( false ), newCommandAvailable_( false )
    {}

    virtual void Start() = 0;

    void Stop();

    void Run();

    virtual void OnGetTaskResult( bool success, int errCode, const WorkerTask &workerTask, const std::string &hostIP );

private:
    virtual void NotifyObserver( int event );

    virtual void GetTaskResult( const WorkerTask &workerTask, const std::string &hostIP ) = 0;

private:
    bool stopped_;

    boost::mutex awakeMut_;
    boost::condition_variable awakeCond_;
    bool newCommandAvailable_;
};

class SenderBoost : public boost::enable_shared_from_this< SenderBoost >
{
    typedef boost::array< char, 32 * 1024 > BufferType;

public:
    typedef boost::shared_ptr< GetterBoost > getter_ptr;

public:
    SenderBoost( boost::asio::io_service &io_service,
                 ResultGetter *getter, const WorkerTask &workerTask,
                 const std::string &hostIP )
    : io_service_( io_service ), socket_( io_service ),
     response_( false ), firstRead_( true ),
     getter_( getter ), workerTask_( workerTask ),
     hostIP_( hostIP )
    {}

    void GetTaskResult();

private:
    void HandleConnect( const boost::system::error_code &error );

    void MakeRequest();

    void HandleWrite( const boost::system::error_code &error, size_t bytes_transferred );

    void FirstRead( const boost::system::error_code &error, size_t bytes_transferred );

    void HandleRead( const boost::system::error_code &error, size_t bytes_transferred );

    bool HandleResponse();

private:
    boost::asio::io_service &io_service_;
    tcp::socket socket_;
    BufferType buffer_;
    python_server::Request< BufferType > response_;
    bool firstRead_;
    std::string request_;
    ResultGetter *getter_;
    WorkerTask workerTask_;
    std::string hostIP_;
};

class CommandSenderBoost : public CommandSender
{
public:
    CommandSenderBoost( boost::asio::io_service &io_service,
                        int maxSimultCommandSenders )
    : io_service_( io_service ),
     cmdSenderSem_( maxSimultCommandSenders )
    {}

    virtual void Start();

private:
    virtual void GetTaskResult( const WorkerTask &workerTask, const std::string &hostIP );

    virtual void OnGetTaskResult( bool success, int errCode, const WorkerTask &workerTask, const std::string &hostIP );

private:
    boost::asio::io_service &io_service_;
    python_server::Semaphore cmdSenderSem_;
};

} // namespace master

#endif

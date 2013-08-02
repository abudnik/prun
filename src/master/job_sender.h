#ifndef __JOB_SENDER_H
#define __JOB_SENDER_H

#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include "common/observer.h"

namespace master {

class JobSender : python_server::Observer
{
public:
    JobSender() : stopped_( false ) {}

    virtual void Start() = 0;

    void Stop();

    void Run();

private:
    virtual void NotifyObserver( int event );

private:
    bool stopped_;
};

class JobSenderBoost : public JobSender
{
public:
    JobSenderBoost( boost::asio::io_service &io_service )
    : io_service_( io_service )
    {}

    virtual void Start();

private:
    boost::asio::io_service &io_service_;
};

} // namespace master

#endif

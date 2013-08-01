#ifndef __JOB_SENDER_H
#define __JOB_SENDER_H

#include <boost/asio.hpp>
#include "common/observer.h"

namespace master {

class JobSender : python_server::Observer
{
public:
    void Start();

private:
    virtual void NotifyObserver( int event );

private:
};

class JobSenderBoost : public JobSender
{
public:
    JobSenderBoost( boost::asio::io_service &io_service )
    : io_service_( io_service )
    {}

private:
    boost::asio::io_service &io_service_;
};

} // namespace master

#endif

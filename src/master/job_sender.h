#ifndef __JOB_SENDER_H
#define __JOB_SENDER_H

#include <boost/asio.hpp>

namespace master {

class JobSender
{
public:
    virtual ~JobSender() {}

    virtual void Start() = 0;

private:
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

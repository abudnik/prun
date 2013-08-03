#ifndef __JOB_SENDER_H
#define __JOB_SENDER_H

#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include "common/observer.h"
#include "common/helper.h"

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

    boost::mutex awakeMut_;
    boost::condition_variable awakeCond_;
    bool newJobAvailable_;
};

class JobSenderBoost : public JobSender
{
public:
    JobSenderBoost( boost::asio::io_service &io_service,
					int sendBufferSize, int maxSimultSendingJobs )
    : io_service_( io_service ), sendBufferSize_( sendBufferSize ),
	 maxSimultSendingJobs_( maxSimultSendingJobs ), sendJobsSem_( maxSimultSendingJobs )
    {}

    virtual void Start();

private:
    boost::asio::io_service &io_service_;
	int sendBufferSize_;
	int maxSimultSendingJobs_;
	python_server::Semaphore sendJobsSem_;
};

} // namespace master

#endif

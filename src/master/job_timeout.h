#ifndef __JOB_TIMEOUT_H
#define __JOB_TIMEOUT_H

#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread/mutex.hpp>
#include "common/helper.h"
#include "worker.h"

namespace master {

// hint: don't use boost::asio::deadline_timer due to os timer limitations (~16k or so)

class JobTimeout
{
    typedef std::map< boost::posix_time::ptime, WorkerJob > TimeToJob;

public:
    JobTimeout( boost::asio::io_service &io_service )
    : io_service_( io_service ), stopped_( false )
    {}

    void Start();

    void Stop();

    void Run();

    void PushJob( const WorkerJob &job, int timeout );

private:
    void CheckTimeouts();

private:
    boost::asio::io_service &io_service_;
    bool stopped_;
    python_server::SyncTimer timer_;
    TimeToJob jobs_;
    boost::mutex jobsMut_;
};

} // namespace master

#endif

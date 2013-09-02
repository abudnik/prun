#ifndef __JOB_TIMEOUT_H
#define __JOB_TIMEOUT_H

#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>
#include "common/helper.h"
#include "worker.h"

namespace master {

// hint: don't use boost::asio::deadline_timer due to os timer limitations (~16k or so)

class TimeoutManager
{
    typedef boost::function< void () > Callback;
    typedef std::multimap< boost::posix_time::ptime,
                           Callback > TimeToCallback;

    struct TaskTimeoutHandler
    {
        void HandleTimeout();
        WorkerJob workerJob_;
        std::string hostIP_;
    };

    struct JobTimeoutHandler
    {
        void HandleTimeout();
        int64_t jobId_;
    };
    struct JobQueueTimeoutHandler
    {
        void HandleTimeout();
        int64_t jobId_;
    };

public:
    TimeoutManager( boost::asio::io_service &io_service )
    : io_service_( io_service ), stopped_( false )
    {}

    void Start();

    void Stop();

    void Run();

    void PushJob( int64_t jobId, int jobTimeout, int queueTimeout );
    void PushTask( const WorkerJob &job, const std::string &hostIP, int timeout );

private:
    void CheckTimeouts();

private:
    boost::asio::io_service &io_service_;
    bool stopped_;
    python_server::SyncTimer timer_;
    TimeToCallback jobs_;
    boost::mutex jobsMut_;
};

} // namespace master

#endif

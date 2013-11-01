#ifndef __TIMEOUT_MANAGER_H
#define __TIMEOUT_MANAGER_H

#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>
#include "common/helper.h"
#include "worker.h"
#include "command.h"

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
        WorkerTask workerTask_;
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
    struct StopTaskTimeoutHandler
    {
        void HandleTimeout();
        CommandPtr command_;
        std::string hostIP_;
    };

public:
    TimeoutManager( boost::asio::io_service &io_service )
    : io_service_( io_service ), stopped_( false )
    {}

    void Start();

    void Stop();

    void Run();

    void PushJobQueue( int64_t jobId, int queueTimeout );
    void PushJob( int64_t jobId, int jobTimeout );
    void PushTask( const WorkerTask &task, const std::string &hostIP, int timeout );

    void PushCommand( CommandPtr &command, const std::string &hostIP, int timeout );

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

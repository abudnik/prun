#ifndef __PING_H
#define __PING_H

#include <boost/asio.hpp>
#include "worker_manager.h"
#include "common/helper.h"

namespace master {

class Pinger
{
public:
    Pinger( WorkerManager &workerMgr, int pingTimeout )
    : workerMgr_( workerMgr ), pingTimeout_( pingTimeout )
    {}

    virtual void StartPing() = 0;

    void Stop();

protected:
    WorkerManager &workerMgr_;
    python_server::SyncTimer timer_;
    int pingTimeout_;
};

class PingerBoost : public Pinger
{
public:
    PingerBoost( WorkerManager &workerMgr, boost::asio::io_service &io_service, int pingTimeout )
    : Pinger( workerMgr, pingTimeout ), io_service_( io_service )
    {}

    virtual void StartPing();

private:
    void Run();

private:
    boost::asio::io_service &io_service_;
};

} // namespace master

#endif

#ifndef __PING_H
#define __PING_H

#include <boost/asio.hpp>
#include "worker_manager.h"

namespace master {

class Pinger
{
public:
    Pinger( WorkerManager &workerMgr )
    : workerMgr_( workerMgr )
    {}

    virtual void StartPing() = 0;

private:
    WorkerManager &workerMgr_;
};

class PingerBoost : public Pinger
{
public:
    PingerBoost( WorkerManager &workerMgr, boost::asio::io_service &io_service )
    : Pinger( workerMgr ), io_service_( io_service )
    {}

    virtual void StartPing()
    {
    }

private:
    boost::asio::io_service &io_service_;
};

} // namespace master

#endif

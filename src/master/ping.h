#ifndef __PING_H
#define __PING_H

#include <boost/asio.hpp>
#include <sstream>
#include "worker_manager.h"
#include "common/helper.h"
#include "common/protocol.h"
#include "defines.h"

namespace master {

class Pinger
{
public:
    Pinger( WorkerManager &workerMgr, int pingTimeout )
    : workerMgr_( workerMgr ), pingTimeout_( pingTimeout )
    {
		protocol_ = new python_server::ProtocolJson;
	}

	virtual ~Pinger()
	{
		delete protocol_;
	}

    virtual void StartPing() = 0;

    void Stop();

protected:
    void PingWorkers();
    virtual void PingWorker( Worker *worker ) = 0;

	const std::string &GetHostIP() const
	{
		return workerMgr_.GetHostIP();
	}

private:
    WorkerManager &workerMgr_;

protected:
    python_server::SyncTimer timer_;
    int pingTimeout_;
	python_server::Protocol *protocol_;
};


using boost::asio::ip::udp;

class PingerBoost : public Pinger
{
    typedef std::map< std::string, udp::endpoint > EndpointMap;

public:
    PingerBoost( WorkerManager &workerMgr, boost::asio::io_service &io_service, int pingTimeout )
    : Pinger( workerMgr, pingTimeout ), io_service_( io_service ),
     socket_( io_service, udp::endpoint( udp::v4(), 0 ) ),
     resolver_( io_service )
    {
        std::ostringstream ss;
        ss << master::NODE_UDP_PORT;
        port_ = ss.str();
    }

    virtual void StartPing();

private:
    void Run();
    virtual void PingWorker( Worker *worker );

private:
    boost::asio::io_service &io_service_;
    udp::socket socket_;
    udp::resolver resolver_;
    std::string port_;
    EndpointMap endpoints_;
};

} // namespace master

#endif

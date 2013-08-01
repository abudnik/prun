#ifndef __PING_H
#define __PING_H

#include <boost/asio.hpp>
#include <sstream>
#include "common/helper.h"
#include "common/protocol.h"
#include "defines.h"
#include "worker.h"

namespace master {

class Pinger
{
public:
    Pinger( int pingTimeout, int maxDroped )
    : pingTimeout_( pingTimeout ), maxDroped_( maxDroped ),
     numPings_( 0 )
    {
		protocol_ = new python_server::ProtocolJson;
	}

	virtual ~Pinger()
	{
		delete protocol_;
	}

    virtual void StartPing() = 0;

    void Stop();

    void Run();

protected:
    void PingWorkers();
    virtual void PingWorker( Worker *worker ) = 0;

    void CheckDropedPingResponses();

    void OnWorkerIPResolve( Worker *worker, const std::string &ip );

protected:
    python_server::SyncTimer timer_;
    int pingTimeout_;
    int maxDroped_;
    int numPings_;
	python_server::Protocol *protocol_;
};


using boost::asio::ip::udp;

class PingerBoost : public Pinger
{
    typedef std::map< std::string, udp::endpoint > EndpointMap;

public:
    PingerBoost( boost::asio::io_service &io_service, int pingTimeout, int maxDroped )
    : Pinger( pingTimeout, maxDroped ),
     io_service_( io_service ),
     socket_( io_service, udp::endpoint( udp::v4(), 0 ) ),
     resolver_( io_service )
    {
        std::ostringstream ss;
        ss << master::NODE_UDP_PORT;
        port_ = ss.str();
    }

    virtual void StartPing();

private:
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

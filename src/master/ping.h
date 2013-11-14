#ifndef __PING_H
#define __PING_H

#include <boost/asio.hpp>
#include <sstream>
#include "common/helper.h"
#include "common/protocol.h"
#include "common/config.h"
#include "defines.h"
#include "worker.h"

namespace master {

class Pinger
{
public:
    Pinger( int pingTimeout, int maxDroped )
    : stopped_( false ),
     pingTimeout_( pingTimeout ), maxDroped_( maxDroped ),
     numPings_( 0 )
    {
        protocol_ = new common::ProtocolJson;
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
    bool stopped_;
    common::SyncTimer timer_;
    int pingTimeout_;
    int maxDroped_;
    int numPings_;
    common::Protocol *protocol_;
};


using boost::asio::ip::udp;

class PingerBoost : public Pinger
{
    typedef std::map< std::string, udp::endpoint > EndpointMap;

public:
    PingerBoost( boost::asio::io_service &io_service, int pingTimeout, int maxDroped )
    : Pinger( pingTimeout, maxDroped ),
     io_service_( io_service ),
     socket_( io_service ),
     resolver_( io_service )
    {
        common::Config &cfg = common::Config::Instance();
        bool ipv6 = cfg.Get<bool>( "ipv6" );

        socket_.open( ipv6 ? udp::v6() : udp::v4() );

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

#ifndef __MASTER_PING_H
#define __MASTER_PING_H

#include <boost/asio.hpp>
#include "common/protocol.h"
#include "common/config.h"
#include "common.h"

namespace worker {

class MasterPing
{
public:
    MasterPing()
    {
        protocol_ = new common::ProtocolJson;
    }

    virtual ~MasterPing()
    {
        delete protocol_;
    }

    virtual void Start() = 0;

protected:
    common::Protocol *protocol_;
};

using boost::asio::ip::udp;

class MasterPingBoost : public MasterPing
{
public:
    MasterPingBoost( boost::asio::io_service &io_service )
    : socket_( io_service )
    {
        common::Config &cfg = common::Config::Instance();
        bool ipv6 = cfg.Get<bool>( "ipv6" );

        socket_.open( ipv6 ? udp::v6() : udp::v4() );
        socket_.bind( udp::endpoint( ipv6 ? udp::v6() : udp::v4(), DEFAULT_UDP_PORT ) );
    }

    virtual void Start();

private:
    void StartReceive();
    void HandleRead( const boost::system::error_code& error, size_t bytes_transferred );

private:
    boost::array< char, 32 * 1024 > buffer_;
    udp::socket socket_;
    udp::endpoint remote_endpoint_;
};

} // namespace worker

#endif

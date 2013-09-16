#ifndef __NODE_PING_H
#define __NODE_PING_H

#include <boost/asio.hpp>
#include "defines.h"

namespace master {

class PingReceiver
{
public:
    virtual ~PingReceiver() {}

    virtual void Start() = 0;

protected:
    void OnNodePing( const std::string &nodeIP, const std::string &msg );
};

using boost::asio::ip::udp;

class PingReceiverBoost : public PingReceiver
{
public:
    PingReceiverBoost( boost::asio::io_service &io_service )
    : socket_( io_service, udp::endpoint( udp::v4(), master::MASTER_UDP_PORT ) )
    {
        memset( buffer_.c_array(), 0, buffer_.size() );
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

} // namespace master

#endif

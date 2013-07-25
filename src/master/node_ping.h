#ifndef __NODE_PING_H
#define __NODE_PING_H

#include <boost/asio.hpp>
#include "worker_manager.h"
#include "defines.h"

namespace master {

class PingReceiver
{
public:
    PingReceiver( WorkerManager &workerMgr )
	: workerMgr_( workerMgr )
    {}

    virtual ~PingReceiver() {}

    virtual void Start() = 0;

protected:
    void OnNodePing( const std::string &node, const std::string &msg );

private:
    WorkerManager &workerMgr_;
};

using boost::asio::ip::udp;

class PingReceiverBoost : public PingReceiver
{
public:
    PingReceiverBoost( WorkerManager &workerMgr, boost::asio::io_service &io_service )
    : PingReceiver( workerMgr ),
	 io_service_( io_service ),
     socket_( io_service, udp::endpoint( udp::v4(), master::MASTER_UDP_PORT ) )
    {}

    virtual void Start();

private:
    void StartReceive();
    void HandleRead( const boost::system::error_code& error, size_t bytes_transferred );

private:
    boost::asio::io_service &io_service_;
    boost::array< char, 32 * 1024 > buffer_;
    udp::socket socket_;
    udp::endpoint remote_endpoint_;
};

} // namespace master

#endif

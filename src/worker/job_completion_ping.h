#ifndef __JOB_COMPLETION_PING_H
#define __JOB_COMPLETION_PING_H

#include <boost/asio.hpp>
#include <sstream>
#include "common/helper.h"
#include "common/protocol.h"
#include "common.h"
#include "job_completion_table.h"

namespace python_server {

class JobCompletionPinger
{
public:
    JobCompletionPinger( int pingTimeout )
    : pingTimeout_( pingTimeout )
    {
		protocol_ = new ProtocolJson;
	}

	virtual ~JobCompletionPinger()
	{
		delete protocol_;
	}

    virtual void StartPing() = 0;

    void Stop();

    void Run();

protected:
    void PingMasters();
    virtual void PingMaster( const JobDescriptor &descr ) = 0;

protected:
    SyncTimer timer_;
    int pingTimeout_;
	Protocol *protocol_;
};


using boost::asio::ip::udp;

class JobCompletionPingerBoost : public JobCompletionPinger
{
public:
    JobCompletionPingerBoost( boost::asio::io_service &io_service, int pingTimeout )
    : JobCompletionPinger( pingTimeout ),
     io_service_( io_service ),
     socket_( io_service, udp::endpoint( udp::v4(), 0 ) )
    {
    }

    virtual void StartPing();

private:
    virtual void PingMaster( const JobDescriptor &descr );

private:
    boost::asio::io_service &io_service_;
    udp::socket socket_;
};

} // namespace python_server

#endif

#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include "job_completion_ping.h"
#include "common/log.h"

namespace python_server {

void JobCompletionPinger::Stop()
{
    timer_.StopWaiting();
}

void JobCompletionPinger::PingMasters()
{
    typedef std::vector< JobDescriptor > Container;
    Container descriptors;
    JobCompletionTable::Instance().GetAll( descriptors );
    typename Container::const_iterator it = descriptors.begin();
    for( ; it != descriptors.end(); ++it )
    {
        PingMaster( *it );
    }
}

void JobCompletionPinger::Run()
{
    do
    {
        PingMasters();
    }
    while( timer_.Wait( pingTimeout_ * 1000 ) );
}


void JobCompletionPingerBoost::StartPing()
{
    io_service_.post( boost::bind( &JobCompletionPinger::Run, this ) );
}

void JobCompletionPingerBoost::PingMaster( const JobDescriptor &descr )
{
	std::string msg;
    protocol_->NodeJobCompletionPing( msg, descr.jobId, descr.taskId );
	PS_LOG( msg );

    udp::endpoint master_endpoint( boost::asio::ip::address::from_string( descr.masterIP ),
                                   DEFAULT_MASTER_UDP_PORT );
    try
    {
        socket_.send_to( boost::asio::buffer( msg ), master_endpoint );
    }
    catch( boost::system::system_error &e )
    {
        PS_LOG( "JobCompletionPingerBoost::PingMaster: send_to failed: " << e.what() << ", host : " << descr.masterIP );
    }
}

} // namespace python_server

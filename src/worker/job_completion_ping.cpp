#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include "job_completion_ping.h"
#include "common/log.h"

namespace worker {

void JobCompletionPinger::Stop()
{
    stopped_ = true;
    timer_.StopWaiting();
}

void JobCompletionPinger::PingMasters()
{
    typedef std::vector< JobDescriptor > Container;
    Container descriptors;
    JobCompletionTable::Instance().GetAll( descriptors );
    Container::const_iterator it = descriptors.begin();
    for( ; it != descriptors.end(); ++it )
    {
        PingMaster( *it );
    }
}

void JobCompletionPinger::Run()
{
    while( !stopped_ )
    {
        timer_.Wait( pingTimeout_ * 1000 );
        PingMasters();
    }
}


void JobCompletionPingerBoost::StartPing()
{
    io_service_.post( boost::bind( &JobCompletionPinger::Run, this ) );
}

void JobCompletionPingerBoost::PingMaster( const JobDescriptor &descr )
{
    std::string msg;
    protocol_->NodeJobCompletionPing( msg, descr.jobId, descr.taskId );
    PLOG( msg );

    boost::asio::ip::address address( boost::asio::ip::address::from_string( descr.masterIP ) );
    udp::endpoint master_endpoint( address, DEFAULT_MASTER_UDP_PORT );
    try
    {
        socket_.send_to( boost::asio::buffer( msg ), master_endpoint );
    }
    catch( boost::system::system_error &e )
    {
        PLOG_ERR( "JobCompletionPingerBoost::PingMaster: send_to failed: " << e.what() << ", host : " << descr.masterIP );
    }
}

} // namespace worker

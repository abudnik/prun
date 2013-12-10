/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2013 Andrey Budnik <budnik27@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

===========================================================================
*/

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

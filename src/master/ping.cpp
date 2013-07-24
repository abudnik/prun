#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include "ping.h"
#include "common/log.h"

namespace master {

void Pinger::Stop()
{
    timer_.StopWaiting();
}

void PingerBoost::StartPing()
{
    io_service_.post( boost::bind( &PingerBoost::Run, this ) );
}

void PingerBoost::Run()
{
    do
    {
    }
    while( timer_.Wait( pingTimeout_ * 1000 ) );
}

} // namespace master

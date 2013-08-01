#include "job_sender.h"
#include "sheduler.h"

namespace master {

void JobSender::Start()
{
	Sheduler::Instance().Subscribe( this );
}

void JobSender::NotifyObserver( int event )
{
}

} // namespace master

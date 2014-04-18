#include "master/command.h"
#include "master/timeout_manager.h"

using namespace std;

namespace master {

struct MockCommand : Command
{
    MockCommand() : Command( "mock" ) {}
    virtual int GetRepeatDelay() const { return 0; }
    virtual void OnCompletion( int errCode, const std::string &hostIP ) {}
};

struct MockTimeoutManager : ITimeoutManager
{
    virtual void PushJobQueue( int64_t jobId, int queueTimeout ) {}
    virtual void PushJob( int64_t jobId, int jobTimeout ) {}
    virtual void PushTask( const WorkerTask &task, const std::string &hostIP, int timeout ) {}
    virtual void PushCommand( CommandPtr &command, const std::string &hostIP, int delay ) {}
};

} // namespace master

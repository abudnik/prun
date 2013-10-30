#ifndef __WORKER_COMMAND_H
#define __WORKER_COMMAND_H

#include "command.h"
#include "worker.h"

namespace master {

class StopTaskCommand : public Command
{
public:
    StopTaskCommand()
    : Command( "stop_task" )
    {}

private:
    virtual void OnCompletion( int errCode, const std::string &hostIP )
    {
    }
};

} // namespace master

#endif

#ifndef __WORKER_COMMAND_H
#define __WORKER_COMMAND_H

#include "common/log.h"
#include "command.h"

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
        PS_LOG( "Stopping task on worker " << hostIP << ", errCode=" << errCode );
    }

    virtual int GetRepeatDelay() const
    {
        return REPEAT_DELAY;
    }

private:
    const static int REPEAT_DELAY = 60; // 60 sec
};

class StopPreviousJobsCommand : public Command
{
public:
    StopPreviousJobsCommand()
    : Command( "stop_prev" )
    {}

private:
    virtual void OnCompletion( int errCode, const std::string &hostIP )
    {
        PS_LOG( "Stopping previous jobs on worker " << hostIP << ", errCode=" << errCode );
    }

    virtual int GetRepeatDelay() const
    {
        return REPEAT_DELAY;
    }

private:
    const static int REPEAT_DELAY = 60; // 60 sec
};

} // namespace master

#endif

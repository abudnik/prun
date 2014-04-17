#include "master/command.h"

using namespace std;
using namespace master;

struct MockCommand : Command
{
    MockCommand() : Command( "mock" ) {}
    virtual int GetRepeatDelay() const { return 0; }
    virtual void OnCompletion( int errCode, const std::string &hostIP ) {}
};

#ifndef __COMPUTER_INFO_H
#define __COMPUTER_INFO_H

#include <boost/thread.hpp>

namespace python_server {

class ComputerInfo
{
    ComputerInfo()
    {
        numCPU_ = boost::thread::hardware_concurrency();
    }

public:
    int GetNumCPU() const { return numCPU_; }

    static ComputerInfo &Instance()
    {
        static ComputerInfo instance_;
        return instance_;
    }

private:
    int numCPU_;
};

} // namespace python_server

#endif

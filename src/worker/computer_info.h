#ifndef __COMPUTER_INFO_H
#define __COMPUTER_INFO_H

#include <boost/thread.hpp>

namespace worker {

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

} // namespace worker

#endif

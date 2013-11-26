#ifndef __COMPUTER_INFO_H
#define __COMPUTER_INFO_H

#include <unistd.h>
#include <boost/thread.hpp>

namespace worker {

class ComputerInfo
{
    ComputerInfo()
    {
        numCPU_ = boost::thread::hardware_concurrency();

        long pages = sysconf( _SC_PHYS_PAGES );
        long page_size = sysconf( _SC_PAGE_SIZE );
        memSize_ = pages * page_size;
    }

public:
    int GetNumCPU() const { return numCPU_; }

    int64_t GetPhysicalMemory() const { return memSize_; }

    static ComputerInfo &Instance()
    {
        static ComputerInfo instance_;
        return instance_;
    }

private:
    int numCPU_;
    int64_t memSize_;
};

} // namespace worker

#endif

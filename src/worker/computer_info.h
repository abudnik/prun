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

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

#ifndef __NODE_STATE_H
#define __NODE_STATE_H

#include "worker.h"

namespace master {

class NodeState
{
public:
    NodeState()
    : numBusyCPU_( 0 )
    {}

    void Reset()
    {
        numBusyCPU_ = 0;
    }

    void AllocCPU( int numCPU ) { numBusyCPU_ += numCPU; }
    void FreeCPU( int numCPU ) { numBusyCPU_ -= numCPU; }

    int GetNumBusyCPU() const { return numBusyCPU_; }
    int GetNumFreeCPU() const { return worker_ ? worker_->GetNumCPU() - numBusyCPU_ : 0; }
    void SetWorker( WorkerPtr &w ) { worker_ = w; }
    WorkerPtr &GetWorker() { return worker_; }
    const WorkerPtr &GetWorker() const { return worker_; }

private:
    int numBusyCPU_;
    WorkerPtr worker_;

};

} // namespace master

#endif

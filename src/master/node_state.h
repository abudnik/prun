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
    int GetNumFreeCPU() const { return worker_->GetNumCPU() - numBusyCPU_; }
    void SetWorker( WorkerPtr &w ) { worker_ = w; }
    WorkerPtr &GetWorker() { return worker_; }

private:
    int numBusyCPU_;
    WorkerPtr worker_;

};

} // namespace master

#endif

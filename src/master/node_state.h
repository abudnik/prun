#ifndef __NODE_STATE_H
#define __NODE_STATE_H

#include "worker.h"

namespace master {

class NodeState
{
public:
    NodeState()
    : numBusyCPU_( 0 ),
     worker_( NULL )
    {}

    void Reset()
    {
        numBusyCPU_ = 0;
    }

    int GetNumBusyCPU() const { return numBusyCPU_; }
    void SetNumBusyCPU( int num ) { numBusyCPU_ = num; }
    int GetNumFreeCPU() const { return worker_->GetNumCPU() - numBusyCPU_; }
    void SetWorker( Worker *w ) { worker_ = w; }
    Worker *GetWorker() const { return worker_; }

private:
    int numBusyCPU_;
    Worker *worker_;

};

} // namespace master

#endif

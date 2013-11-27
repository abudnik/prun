#ifndef __WORKER_PRIORITY_H
#define __WORKER_PRIORITY_H

#include <vector>
#include <iterator>
#include "node_state.h"


namespace master {

struct CompareByCPU
{
    bool operator() ( const NodeState *a, const NodeState *b ) const
    {
        return a->GetNumFreeCPU() < b->GetNumFreeCPU();
    }
};

struct CompareByCPUandMemory
{
    bool operator() ( const NodeState *a, const NodeState *b ) const
    {
        if ( a->GetNumFreeCPU() < b->GetNumFreeCPU() )
            return true;

        if ( a->GetNumFreeCPU() == b->GetNumFreeCPU() )
        {
            const WorkerPtr &wa = a->GetWorker();
            const WorkerPtr &wb = b->GetWorker();
            return wa && wb && ( wa->GetMemorySize() < wb->GetMemorySize() );
        }
        return false;
    }
};

class WorkerPriority
{
private:
    typedef std::vector< NodeState * > Container;

public:
    template< class MapIterator >
    void Sort( MapIterator first, MapIterator last, size_t num )
    {
        Fill( first, last, num );
        // no sort
    }

    template< class MapIterator, class Comparator >
    void Sort( MapIterator first, MapIterator last, size_t num, Comparator comp )
    {
        Fill( first, last, num );
        DoSort( comp );
    }

    typedef Container::iterator iterator;
    iterator Begin() { return workers_.begin(); }
    iterator End() { return workers_.end(); }

private:
    template< class MapIterator >
    void Fill( MapIterator first, MapIterator last, size_t num )
    {
        workers_.resize( num );
        size_t i = 0;
        for( MapIterator it = first; it != last; ++it )
        {
            workers_[ i++ ] = &it->second;
        }
    }

    template< class Comparator >
    void DoSort( Comparator comp )
    {
        std::sort( workers_.begin(), workers_.end(), comp );
    }

private:
    Container workers_;
};

} // namespace master

#endif

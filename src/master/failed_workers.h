#ifndef __FAILED_WORKERS_H
#define __FAILED_WORKERS_H

#include "common/log.h"

class FailedWorkers
{
public:
    void Add( int64_t jobId, const std::string &hostIP )
    {
        failedWorkers_[ jobId ].insert( hostIP );
    }

    bool Delete( int64_t jobId )
    {
        std::map< int64_t, std::set< std::string > >::iterator it_failed(
            failedWorkers_.find( jobId )
        );
        if ( it_failed != failedWorkers_.end() )
        {
            size_t numFailed = it_failed->second.size();
            failedWorkers_.erase( it_failed );
            PS_LOG( "Scheduler::RemoveJob: jobId=" << jobId << ", num failed workers=" << numFailed );
            return true;
        }
        return false;
    }

    bool IsWorkerFailedJob( const std::string &hostIP, int64_t jobId ) const
    {
        std::map< int64_t, std::set< std::string > >::const_iterator it = failedWorkers_.find( jobId );
        if ( it == failedWorkers_.end() )
            return false;

        const std::set< std::string > &ips = it->second;
        return ips.find( hostIP ) != ips.end();
    }

    size_t GetFailedNodesCnt( int64_t jobId ) const
    {
        std::map< int64_t, std::set< std::string > >::const_iterator it = failedWorkers_.find( jobId );
        if ( it == failedWorkers_.end() )
            return 0;

        return it->second.size();
    }

    size_t GetFailedJobsCnt() const
    {
        return failedWorkers_.size();
    }

private:
    std::map< int64_t, std::set< std::string > > failedWorkers_; // job_id -> set(worker_ip)
};

#endif

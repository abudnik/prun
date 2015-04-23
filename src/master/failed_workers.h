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

#ifndef __FAILED_WORKERS_H
#define __FAILED_WORKERS_H

#include "common/log.h"
#include "worker.h"

namespace master {

class FailedWorkers
{
public:
    bool Add( int64_t jobId, const std::string &hostIP )
    {
        return failedWorkers_[ jobId ].insert( hostIP ).second;
    }

    void Add( const WorkerJob &workerJob, const std::string &hostIP )
    {
        std::set<int64_t> jobs;
        workerJob.GetJobs( jobs );
        for( auto jobId : jobs )
        {
            failedWorkers_[ jobId ].insert( hostIP );
        }
    }

    bool Delete( int64_t jobId )
    {
        auto it_failed = failedWorkers_.find( jobId );
        if ( it_failed != failedWorkers_.end() )
        {
            size_t numFailed = it_failed->second.size();
            failedWorkers_.erase( it_failed );
            PLOG( "FailedWorkers::Delete: jobId=" << jobId << ", num failed workers=" << numFailed );
            return true;
        }
        return false;
    }

    bool IsWorkerFailedJob( const std::string &hostIP, int64_t jobId ) const
    {
        auto it = failedWorkers_.find( jobId );
        if ( it == failedWorkers_.end() )
            return false;

        const std::set< std::string > &ips = it->second;
        return ips.find( hostIP ) != ips.end();
    }

    size_t GetFailedNodesCnt( int64_t jobId ) const
    {
        auto it = failedWorkers_.find( jobId );
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

} // namespace master

#endif

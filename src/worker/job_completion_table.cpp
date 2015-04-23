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

#include "job_completion_table.h"

namespace worker {

bool JobDescriptor::Equal( const JobDescriptor &descr ) const
{
    return jobId == descr.jobId && taskId == descr.taskId && masterIP == descr.masterIP;
}


void JobCompletionTable::Set( const JobDescriptor &descr, const JobCompletionStat &stat )
{
    boost::upgrade_lock< boost::shared_mutex > lock( tableMut_ );
    boost::upgrade_to_unique_lock< boost::shared_mutex > uniqueLock( lock );
    table_[ descr ] = stat;
}

bool JobCompletionTable::Get( const JobDescriptor &descr, JobCompletionStat &stat )
{
    boost::shared_lock< boost::shared_mutex > lock( tableMut_ );
    auto it = table_.find( descr );
    if ( it != table_.end() )
    {
        stat = it->second;
        return true;
    }
    return false;
}

bool JobCompletionTable::Erase( const JobDescriptor &descr )
{
    boost::upgrade_lock< boost::shared_mutex > lock( tableMut_ );
    boost::upgrade_to_unique_lock< boost::shared_mutex > uniqueLock( lock );
    return table_.erase( descr ) > 0;
}

bool JobCompletionTable::ErasePending( const JobDescriptor &descr )
{
    boost::upgrade_lock< boost::shared_mutex > lock( tableMut_ );
    boost::upgrade_to_unique_lock< boost::shared_mutex > uniqueLock( lock );

    bool removed = false;
    for( auto it = table_.begin(); it != table_.end(); )
    {
        const JobDescriptor &d = it->first;
        if ( d.Equal( descr ) && ( d.masterId != descr.masterId ) )
        {
            table_.erase( it++ );
            removed = true;
            continue;
        }
        ++it;
    }

    return removed;
}

} // namespace worker

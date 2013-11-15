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
    Table::const_iterator it = table_.find( descr );
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
    Table::iterator it = table_.begin();
    for( ; it != table_.end(); )
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

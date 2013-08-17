#include "job_completion_table.h"

namespace python_server {


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

} // namespace python_server

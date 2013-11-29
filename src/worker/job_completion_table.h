#ifndef __JOB_COMPLETION_TABLE_H
#define __JOB_COMPLETION_TABLE_H

#include <map>
#include <string>
#include <boost/thread/locks.hpp>  
#include <boost/thread/shared_mutex.hpp> 

namespace worker {

struct JobCompletionStat
{
    int errCode;
    int64_t execTime;
};

struct JobDescriptor
{
    int64_t jobId;
    int taskId;
    std::string masterIP;
    std::string masterId;

    bool Equal( const JobDescriptor &descr ) const;
};

class JobCompletionTable
{
    struct JobDescriptorComparator
    {
        bool operator() ( const JobDescriptor &a, const JobDescriptor &b ) const
        {
            if ( a.jobId < b.jobId )
                return true;

            if ( a.jobId == b.jobId )
            {
                if ( a.taskId == b.taskId )
                {
                    int cmp = a.masterIP.compare( b.masterIP );
                    if ( !cmp )
                    {
                        return a.masterId.compare( b.masterId ) < 0;
                    }
                    return cmp < 0;
                }
                return a.taskId < b.taskId;
            }

            return false;
        }
    };

    typedef std::map< JobDescriptor, JobCompletionStat, JobDescriptorComparator > Table;

public:
    static JobCompletionTable &Instance()
    {
        static JobCompletionTable instance_;
        return instance_;
    }

    void Set( const JobDescriptor &descr, const JobCompletionStat &stat );

    bool Get( const JobDescriptor &descr, JobCompletionStat &stat );

    template< class Container >
    void GetAll( Container &descriptors )
    {
        if ( table_.empty() )
            return;
        boost::shared_lock< boost::shared_mutex > lock( tableMut_ );
        Table::const_iterator it = table_.begin();
        for( ; it != table_.end(); ++it )
        {
            descriptors.push_back( it->first );
        }
    }

    bool Erase( const JobDescriptor &descr );

    bool ErasePending( const JobDescriptor &descr );

private:
    Table table_;
    boost::shared_mutex tableMut_;
};

} // namespace worker

#endif

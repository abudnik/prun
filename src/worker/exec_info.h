#ifndef __EXEC_INFO_H
#define __EXEC_INFO_H

#include <stdint.h> // int64_t
#include <list>
#include <boost/function.hpp>
#include <boost/thread.hpp>


namespace python_server {

struct ExecInfo
{
    int64_t jobId_;
    int taskId_;
    boost::function< void () > callback_;
};

class ExecTable
{
    typedef std::list< ExecInfo > Container;

public:
    void Add( const ExecInfo &execInfo )
    {
        boost::unique_lock< boost::mutex > lock( mut_ );
        table_.push_back( execInfo );
    }

    void Delete( int64_t jobId, int taskId )
    {
        boost::unique_lock< boost::mutex > lock( mut_ );
        Container::iterator it = table_.begin();
        for( ; it != table_.end(); ++it )
        {
            const ExecInfo &execInfo = *it;
            if ( execInfo.jobId_ == jobId && execInfo.taskId_ == taskId )
            {
                table_.erase( it );
                break;
            }
        }
    }

    void Clear()
    {
        boost::unique_lock< boost::mutex > lock( mut_ );
        Container::iterator it = table_.begin();
        for( ; it != table_.end(); ++it )
        {
            const ExecInfo &execInfo = *it;
            if ( execInfo.callback_ )
                execInfo.callback_();
        }
        table_.clear();
    }

private:
    Container table_;
    boost::mutex mut_;
};

} // namespace python_server

#endif

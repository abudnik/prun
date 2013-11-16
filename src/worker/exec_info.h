#ifndef __EXEC_INFO_H
#define __EXEC_INFO_H

#include <stdint.h> // int64_t
#include <list>
#include <boost/function.hpp>
#include <boost/thread.hpp>


namespace worker {

struct ExecInfo
{
    int64_t jobId_;
    int taskId_;
    std::string masterId_;
    pid_t pid_; // used in pyexec
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

    bool Delete( int64_t jobId, int taskId, const std::string &masterId )
    {
        boost::unique_lock< boost::mutex > lock( mut_ );
        Container::iterator it = table_.begin();
        for( ; it != table_.end(); ++it )
        {
            const ExecInfo &execInfo = *it;
            if ( execInfo.jobId_ == jobId &&
                 execInfo.taskId_ == taskId &&
                 execInfo.masterId_ == masterId )
            {
                table_.erase( it );
                return true;
            }
        }
        return false;
    }

    bool Contains( int64_t jobId, int taskId, const std::string &masterId )
    {
        boost::unique_lock< boost::mutex > lock( mut_ );
        Container::const_iterator it = table_.begin();
        for( ; it != table_.end(); ++it )
        {
            const ExecInfo &execInfo = *it;
            if ( execInfo.jobId_ == jobId &&
                 execInfo.taskId_ == taskId &&
                 execInfo.masterId_ == masterId )
                return true;
        }
        return false;
    }

    bool Find( int64_t jobId, int taskId, const std::string &masterId, ExecInfo &info )
    {
        boost::unique_lock< boost::mutex > lock( mut_ );
        Container::const_iterator it = table_.begin();
        for( ; it != table_.end(); ++it )
        {
            const ExecInfo &execInfo = *it;
            if ( execInfo.jobId_ == jobId &&
                 execInfo.taskId_ == taskId &&
                 execInfo.masterId_ == masterId )
            {
                info = execInfo;
                return true;
            }
        }
        return false;
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

} // namespace worker

#endif

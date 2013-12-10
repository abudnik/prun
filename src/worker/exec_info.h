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

    template< typename ArgContainer >
    void Get( ArgContainer &table )
    {
        boost::unique_lock< boost::mutex > lock( mut_ );
        Container::iterator it = table_.begin();
        for( ; it != table_.end(); ++it )
        {
            table.push_back( *it );
        }
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
        Container table( table_ );
        lock.unlock();

        // run registered callbacks
        Container::iterator it = table.begin();
        for( ; it != table.end(); ++it )
        {
            const ExecInfo &execInfo = *it;
            if ( execInfo.callback_ )
                execInfo.callback_();
        }

        lock.lock();
        table_.clear();
    }

private:
    Container table_;
    boost::mutex mut_;
};

} // namespace worker

#endif

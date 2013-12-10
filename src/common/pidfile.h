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

#ifndef __PIDFILE_H
#define __PIDFILE_H

#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include "log.h"

using namespace boost::interprocess;

namespace common {

class Pidfile
{
public:
    Pidfile( const char *filePath )
    : filePath_( filePath )
    {
        try
        {
            bool fileExists = boost::filesystem::exists( filePath );

            file_.open( filePath );
            if ( !file_.is_open() )
            {
                PLOG_ERR( "can't open pid_file=" << filePath_ );
                exit( 1 );
            }

            file_lock f_lock( filePath );
            if ( !f_lock.try_lock() )
            {
                PLOG_ERR( "can't exclusively lock pid_file=" << filePath_ );
                exit( 1 );
            }

            f_lock_.swap( f_lock );

            file_ << getpid();
            file_.flush();

            afterFail_ = fileExists;
            if ( afterFail_ )
            {
                PLOG_WRN( "previous process terminated abnormally" );
            }
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "Pidfile failed: " << e.what() );
            exit( 1 );
        }
    }

    ~Pidfile()
    {
        try
        {
            f_lock_.unlock();
            file_.close();
            boost::filesystem::remove( filePath_ );
        }
        catch(...)
        {
            PLOG_ERR( "exception in ~Pidfile()" );
        }
    }

    bool AfterFail() const { return afterFail_; }

private:
    bool afterFail_;
    std::string filePath_;
    file_lock f_lock_;
    std::ofstream file_;
};

} // namespace common

#endif

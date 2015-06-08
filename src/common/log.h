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

#ifndef __LOG_H
#define __LOG_H

#include <sstream>

#define PLOG_PREPARE_MESSAGE( MSG ) \
    std::ostringstream os;  \
    os << MSG << "  (from " << __FILE__ << ':' << __LINE__ << ')';

#define PLOG( MSG )\
    if ( common::logger::CheckLogLevel( common::logger::LogLevel::INFO ) ) { \
        PLOG_PREPARE_MESSAGE( MSG );                                    \
        common::logger::Log( os.str().c_str() );                        \
    }

#define PLOG_DBG( MSG )\
    if ( common::logger::CheckLogLevel( common::logger::LogLevel::DEBUG ) ) { \
        PLOG_PREPARE_MESSAGE( MSG );                                    \
        common::logger::LogDebug( os.str().c_str() );                   \
    }

#define PLOG_WRN( MSG )\
    if ( common::logger::CheckLogLevel( common::logger::LogLevel::WARNING ) ) { \
        PLOG_PREPARE_MESSAGE( MSG );                                    \
        common::logger::LogWarning( os.str().c_str() );                 \
    }

#define PLOG_ERR( MSG )\
    if ( common::logger::CheckLogLevel( common::logger::LogLevel::ERROR ) ) { \
        PLOG_PREPARE_MESSAGE( MSG );                                    \
        common::logger::LogError( os.str().c_str() );                   \
    }

namespace common {

namespace logger
{

enum LogLevel
{
    DEBUG   = 0,
    INFO    = 1,
    WARNING = 2,
    ERROR   = 3
};

void InitLogger( bool useSyslog, const char *serviceName, const char *level );

void ShutdownLogger();

void Log( const char *msg );

void LogDebug( const char *msg );

void LogWarning( const char *msg );

void LogError( const char *msg );

bool CheckLogLevel( LogLevel want );

} // namespace logger

} // namespace common

#endif

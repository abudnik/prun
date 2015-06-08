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

#include <iostream>
#include <syslog.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include "log.h"


namespace common {

namespace logger
{

bool useSyslog = false;
const char *serviceName = "";
bool isTerminal = false;
LogLevel logLevel;

void InitLogger( bool useSyslog, const char *serviceName, const char *level )
{
    logger::useSyslog = useSyslog;
    logger::serviceName = serviceName;

    if ( !strcasecmp( level, "debug" ) )
        logLevel = LogLevel::DEBUG;
    else
    if ( !strcasecmp( level, "info" ) )
        logLevel = LogLevel::INFO;
    else
    if ( !strcasecmp( level, "warning" ) )
        logLevel = LogLevel::WARNING;
    else
    if ( !strcasecmp( level, "error" ) )
        logLevel = LogLevel::ERROR;
    else
    {
        std::cout << "InitLogger: unknown log level '" << level << "', using 'debug' by default" << std::endl;
        logLevel = LogLevel::DEBUG;
    }

    if ( useSyslog )
    {
        openlog( serviceName, LOG_CONS, LOG_DAEMON );
    }

    isTerminal = isatty( fileno( stdout ) );
}

void ShutdownLogger()
{
    if ( useSyslog )
    {
        closelog();
    }
}

void Print( char level, const char *msg )
{
    char buf[32];
    time_t tmNow( time( nullptr ) );

    tm tmWhen;
    memset( &tmWhen, 0, sizeof( tmWhen ) );
    localtime_r( &tmNow, &tmWhen );
    snprintf( buf, sizeof(buf), " %02d.%02d %02d:%02d:%02d: ",
              tmWhen.tm_mday, tmWhen.tm_mon + 1, tmWhen.tm_hour, tmWhen.tm_min, tmWhen.tm_sec );

    if ( isTerminal )
    {
        switch( level )
        {
            case 'W': std::cout << "\033[33m"; break;
            case 'E': std::cout << "\033[31m"; break;
        }
    }

    std::cout << '<' << level << ' ' << serviceName << buf << msg << std::endl;

    if ( isTerminal )
        std::cout << "\033[0m";
}

void Log( const char *msg )
{
    if ( useSyslog )
    {
        syslog( LOG_INFO, "%s", msg );
    }
    else
    {
        Print( 'I', msg );
    }
}

void LogDebug( const char *msg )
{
    if ( useSyslog )
    {
        syslog( LOG_DEBUG, "%s", msg );
    }
    else
    {
        Print( 'D', msg );
    }
}

void LogWarning( const char *msg )
{
    if ( useSyslog )
    {
        syslog( LOG_WARNING, "%s", msg );
    }
    else
    {
        Print( 'W', msg );
    }
}

void LogError( const char *msg )
{
    if ( useSyslog )
    {
        syslog( LOG_ERR, "%s", msg );
    }
    else
    {
        Print( 'E', msg );
    }
}

bool CheckLogLevel( LogLevel want )
{
    return static_cast<int>(logLevel) <= static_cast<int>(want);
}

} // namespace logger

} // namespace common

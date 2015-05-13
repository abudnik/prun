/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2015 Andrey Budnik <budnik27@gmail.com>

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

#ifndef __CRON_H
#define __CRON_H

#include <string>
#include <bitset>
#include <chrono>
#include <stdexcept>
#include <cstring>
#include "log.h"


namespace common {

template< size_t MinValue, size_t MaxValue >
class CronField
{
public:
    void AddValue( size_t v )
    {
        //PLOG( "value : " << v << " of " << MaxValue );
        FixArg( v );
        time_val_.set( v );
    };

    void AddRange( size_t first, size_t last )
    {
        //PLOG( "range : " << first << ',' << last << " of " << MaxValue );
        if ( first > last )
            std::swap( first, last );

        if ( first < MinValue )
        {
            PLOG_ERR( "CronField::AddRange: first < MinValue (" << first << " < " << last << ')' );
            throw std::out_of_range( "CronField::AddRange: first < MinValue" );
        }

        FixArg( first );
        FixArg( last );

        for( size_t i = first; i <= last; ++i )
            time_val_.set( i );
    }

    void AddPeriod( size_t first, size_t last, size_t period )
    {
        //PLOG( "period : " << first << ',' << last << " of " << MaxValue << " with " << period );
        if ( first > last )
            std::swap( first, last );

        if ( first < MinValue )
        {
            PLOG_ERR( "CronField::AddPeriod: first < MinValue (" << first << " < " << last << ')' );
            throw std::out_of_range( "CronField::AddPeriod: first < MinValue" );
        }

        if ( period == 0 )
        {
            PLOG_ERR( "CronField::AddPeriod: period == 0" );
            throw std::out_of_range( "CronField::AddPeriod: period == 0" );
        }

        FixArg( first );
        FixArg( last );

        time_val_.reset();
        for( size_t i = first; i <= last; i += period )
            time_val_.set( i );
    }

    void SetAll()
    {
        //PLOG( "all" );
        time_val_.set();
    }

    void SetAllPeriod( size_t period )
    {
        //PLOG( "all period " << period );
        if ( period == 0 )
        {
            PLOG_ERR( "CronField::AddPeriod: period == 0" );
            throw std::out_of_range( "CronField::AddPeriod: period == 0" );
        }

        size_t first = MinValue, last = MaxValue;
        FixArg( first );
        FixArg( last );

        time_val_.reset();
        for( size_t i = first; i <= last; i += period )
            time_val_.set( i );
    }

    bool Has( size_t pos ) const
    {
        FixArg( pos );
        return time_val_.test( pos );
    }

    bool All() const
    {
        return time_val_.all();
    }

    void Print() const
    {
        PLOG( MinValue << ',' << MaxValue << ": " << time_val_.to_string() );
    }

private:
    void FixArg( size_t &arg ) const
    {
        static_assert( MaxValue > MinValue, "MaxValue > MinValue" );
        if ( MinValue > 0 )
            arg -= MinValue;
    }

private:
    std::bitset< MaxValue + 1 > time_val_;
};

typedef CronField<0, 59> CronMinutes;
typedef CronField<0, 23> CronHours;
typedef CronField<1, 31> CronDays;
typedef CronField<1, 12> CronMonths;
typedef CronField<0, 6> CronDaysOfWeek;

inline time_t CreateDateTime( int year, int month, int day, int hour, int minute )
{
    tm current;
    memset( &current, 0, sizeof( current ) );

    current.tm_year = year - 1900;
    current.tm_mon = month - 1;
    current.tm_mday = day;
    current.tm_hour = hour;
    current.tm_min = minute;

    return mktime( &current );
}

class CronJob
{
public:
    typedef std::string::const_iterator Iterator;

    CronJob();

    bool Parse( const std::string &cmd ); // NB: may throw an exception

    typedef std::chrono::system_clock::time_point ptime;
    ptime Next( ptime now ) const;

    operator bool () const;

private:
    CronMinutes minute_;
    CronHours hour_;
    CronDays day_month_;
    CronMonths month_;
    CronDaysOfWeek day_week_;
    bool valid_;
};

} // namespace common

#endif

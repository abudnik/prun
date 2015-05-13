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

#include "cron.h"
#include <boost/spirit/include/qi.hpp>
#include <boost/bind.hpp>

using namespace common;


namespace qi = boost::spirit::qi;

template< typename Iterator, typename Field >
struct CronFieldGrammar : qi::grammar< Iterator >
{
public:
    CronFieldGrammar( Field &field )
    : CronFieldGrammar::base_type( root ),
     field_( field )
    {
        root = star | range >> *(',' >> range);

        star = qi::lit( '*' ) [boost::bind(&Field::SetAll, &field_)] >>
            -( '/' >> value_ [boost::bind(&Field::SetAllPeriod, &field, _1)] );

        range = ( value_ [boost::bind(&CronFieldGrammar::OnBeginRange, this, _1)] >> '-' >>
                  value_ [boost::bind(&CronFieldGrammar::OnEndRange, this, _1)] >>
                  -( '/' >> value_ [boost::bind(&CronFieldGrammar::OnPeriod, this, _1)] ) ) |
            single_value;

        single_value = value_ [boost::bind(&Field::AddValue, &field_, _1)];
    }

    // NB: if amount of OnEvent() methods below exceeds reasonable limit, then move them to special Handler object
    void OnBeginRange( size_t val )
    {
        begin_range_ = val;
    }

    void OnEndRange( size_t val )
    {
        end_range_ = val;
        field_.AddRange( begin_range_, end_range_ );
    }

    void OnPeriod( size_t val )
    {
        field_.AddPeriod( begin_range_, end_range_, val );
    }

    Field &field_;
    qi::rule<Iterator> root, star, range, single_value;
    qi::uint_parser<unsigned char, 10, 1, 2> value_;
    size_t begin_range_, end_range_;
};

template<typename Field>
bool ParseField( Field &field, CronJob::Iterator &it, const std::string &cmd, int field_num )
{
    try
    {
        CronFieldGrammar< CronJob::Iterator, Field > grammar( field );
        const bool success = qi::phrase_parse( it, cmd.cend(), grammar, boost::spirit::ascii::space );
        if ( !success )
        {
            if ( it == cmd.cend() )
            {
                PLOG_ERR( "ParseField: expected exactly 5 cron fields, but only " << field_num <<
                          " found: cmd='" << cmd << '\'' );
            }
            else
            {
                PLOG_ERR( "ParseField: failed parse crontab at symbol " << std::distance( cmd.cbegin(), it ) <<
                          " on parsing field " << field_num << ", cmd='" << cmd << '\'' );
            }
        }
        return success;
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ParseField: exception: " << e.what() );
    }
    return false;
}

static time_t CreateDate( int year, int month, int day )
{
    return CreateDateTime( year, month, day, 0, 0 );
}

static int GetDaysInMonth( int year, int month )
{
    int next_year = year;
    if ( month == 12 )
        ++next_year;
    int next_month = ( month + 1 ) % 12;

    time_t current = CreateDate( year, month, 1 );
    time_t next = CreateDate( next_year, next_month, 1 );
    return ( next - current ) / ( 60 * 60 * 24 );
}

static int GetDayOfWeek( int year, int month, int day )
{
    time_t t = CreateDate( year, month, day );
    return localtime( &t )->tm_wday;
}

time_t CreateDateTime( int year, int month, int day, int hour, int minute )
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

bool CronJob::Parse( const std::string &cmd ) // NB: may throw an exception
{
    //PLOG( "CronJob::Parse: " << cmd );

    auto it_beg = cmd.cbegin();
    auto it_end = cmd.cend();

    if ( !ParseField( minute_, it_beg, cmd, 1 ) )
        return false;
    if ( !ParseField( hour_, it_beg, cmd, 2 ) )
        return false;
    if ( !ParseField( day_month_, it_beg, cmd, 3 ) )
        return false;
    if ( !ParseField( month_, it_beg, cmd, 4 ) )
        return false;
    if ( !ParseField( day_week_, it_beg, cmd, 5 ) )
        return false;

    if ( it_beg != it_end )
    {
        PLOG_ERR( "CronJob::Parse: failed parse crontab at symbol " << std::distance( cmd.cbegin(), it_beg ) <<
                  ": cmd='" << cmd << '\'' );
    }
    return it_beg == it_end;
}

CronJob::ptime CronJob::Next( CronJob::ptime now ) const
{
    std::time_t t = std::chrono::system_clock::to_time_t( now );
    tm local = *localtime( &t );
    int year = 1900 + local.tm_year;
    int month = 1 + local.tm_mon;
    int day = local.tm_mday;
    int hour = local.tm_hour;
    int minute = local.tm_min;

    //PLOG( year << ',' << month << ',' << day << ',' << hour << ',' << minute );
    //minute_.Print();
    //hour_.Print();
    //day_month_.Print();
    //month_.Print();
    //day_week_.Print();

    auto UpdateMonth = [&year, &month] ()
        {
            month = ( month + 1 ) % 12;
            if ( month == 1 )
                ++year;
        };

    auto UpdateDay = [&UpdateMonth, &year, &month, &day] ()
        {
            day = ( day + 1 ) % GetDaysInMonth( year, month );
            if ( day == 1 )
                UpdateMonth();
        };

    auto UpdateHour = [&UpdateDay, &hour] ()
        {
            hour = ( hour + 1 ) % 24;
            if ( hour == 0 )
                UpdateDay();
        };

    auto UpdateMinute = [&UpdateHour, &minute] ()
        {
            minute = ( minute + 1 ) % 60;
            if ( minute == 0 )
                UpdateHour();
        };

    bool first = false;
    while( !month_.Has( month ) )
    {
        first = true;
        UpdateMonth();
    }

    std::function< bool( int, int, int ) > DayPredicate;
    if ( day_month_.All() && day_week_.All() )
    {
        DayPredicate = [this] ( int year, int month, int day ) -> bool
            { return !day_month_.Has( day ) && !day_week_.Has( GetDayOfWeek( year, month, day ) ); };
    }
    else
    if ( day_month_.All() )
    {
        DayPredicate = [this] ( int year, int month, int day ) -> bool
            { return !day_week_.Has( GetDayOfWeek( year, month, day ) ); };
    }
    else
    if ( day_week_.All() )
    {
        DayPredicate = [this] ( int year, int month, int day ) -> bool
            { return !day_month_.Has( day ); };
    }
    else
    {
        DayPredicate = [] ( int year, int month, int day ) -> bool
            { return false; };
    }
    day = first ? 1 : day;
    while( DayPredicate( year, month, day ) )
    {
        first = true;
        UpdateDay();
    }

    hour = first ? 0 : hour;
    while( !hour_.Has( hour ) )
    {
        first = true;
        UpdateHour();
    }

    minute = first ? 0 : minute;
    while( !minute_.Has( minute ) )
    {
        //first = true;
        UpdateMinute();
    }

    t = CreateDateTime( year, month, day, hour, minute );
    return std::chrono::system_clock::from_time_t( t );
}

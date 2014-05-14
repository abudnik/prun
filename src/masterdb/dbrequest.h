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

#ifndef __DB_REQUEST_H
#define __DB_REQUEST_H

#include <vector>
#include <string>
#include <cctype>
#include "common/log.h"


namespace masterdb {

class DbRequest
{
public:
    typedef std::vector< std::string > ArgList;

public:
    bool ParseRequest( const std::string &request )
    {
        size_t offset = 1; // skip newline after header
        return ReadType( request, offset ) && ReadArgs( request, offset ) && ReadData( request, offset );
    }

    void Reset()
    {
        type_.clear();
        args_.clear();
        data_.clear();
    }

    const std::string &GetType() const { return type_; }
    const ArgList &GetArgs() const { return args_; }
    const std::string &GetData() const { return data_; }

private:
    bool ReadType( const std::string &request, size_t &offset )
    {
        size_t i = offset;
        char c = '_';
        for( ; i < request.size(); ++i )
        {
            c = request[i];
            if ( c == ' ' )
                break;
            if ( !isalpha( c ) )
            {
                PLOG_ERR( "DbRequest::ReadType: unexpected '" << c << "'" );
                return false;
            }
        }
        type_ = std::string( request, offset, i - 1 );
        offset = ++i;
        return !type_.empty() && ( c == ' ' );
    }

    bool ReadArgs( const std::string &request, size_t &offset )
    {
        std::string arg;
        size_t i = offset;
        size_t pos = offset;
        char c = '_';
        for( ; i < request.size(); ++i )
        {
            c = request[i];
            if ( c == '&' )
            {
                args_.push_back( std::string( request, pos, i - pos ) );
                pos = i + 1;
            }
            if ( c == '$' )
            {
                args_.push_back( std::string( request, pos, i - pos ) );
                break;
            }
            if ( !isalnum( c ) )
            {
                PLOG_ERR( "DbRequest::ReadArgs: unexpected '" << c << "'" );
                return false;
            }
        }
        offset = ++i;
        return c == '$';
    }

    bool ReadData( const std::string &request, size_t &offset )
    {
        data_ = std::string( request, offset, request.size() - offset );
        return true;
    }

private:
    std::string type_;
    ArgList args_;
    std::string data_;
};

} // namespace masterdb

#endif

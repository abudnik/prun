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

#ifndef __REQUEST_H
#define __REQUEST_H

#include <boost/lexical_cast.hpp>
#include "log.h"


namespace common {

template< typename BufferT >
class Request
{
public:
    Request( bool skipHeader )
    : requestLength_( 0 ),
     bytesRead_( 0 ),
     headerOffset_( 0 ),
     skipHeader_( skipHeader )
    {
    }

    void OnRead( BufferT &buf, size_t bytes_transferred )
    {
        unsigned int offset = skipHeader_ ? headerOffset_ : 0;

        request_.append( buf.begin() + offset, buf.begin() + bytes_transferred );

        bytesRead_ += bytes_transferred - offset;
        skipHeader_ = false;
    }

    int OnFirstRead( BufferT &buf, size_t bytes_transferred )
    {
        if ( !ParseRequestHeader( buf, bytes_transferred ) )
            return 0;
        return CheckHeader();
    }

    bool IsReadCompleted() const
    {
        return bytesRead_ >= requestLength_;
    }

    const std::string &GetString() const
    {
        return request_;
    }

    unsigned int GetLength() const
    {
        return requestLength_;
    }

    void Reset( bool skipHeader = true )
    {
        request_.clear();
        requestLength_ = bytesRead_ = headerOffset_ = 0;
        skipHeader_ = skipHeader;
    }

private:
    bool ParseRequestHeader( BufferT &buf, size_t bytes_transferred )
    {
        typename BufferT::iterator buf_end = buf.begin() + bytes_transferred;
        typename BufferT::iterator it = std::find( buf.begin(), buf_end, '\n' );
        if ( it != buf_end )
        {
            headerOffset_ = std::distance( buf.begin(), it );
            if ( headerOffset_ ) // don't append one newline char
                request_.append( buf.begin(), it );
            try
            {
                requestLength_ = boost::lexical_cast<unsigned int>( request_ );
            }
            catch( boost::bad_lexical_cast &e )
            {
                PLOG_ERR( "Reading request length failed: " << e.what() );
            }
            request_.clear();
            return true;
        }
        else
        {
            request_.append( buf.begin(), buf.begin() + bytes_transferred );
        }

        return false;
    }

    int CheckHeader() const
    {
        // TODO: Error codes
        return requestLength_;
    }

private:
    std::string request_;
    unsigned int requestLength_;
    unsigned int bytesRead_;
    unsigned int headerOffset_;
    bool skipHeader_;
};

} // namespace common

#endif

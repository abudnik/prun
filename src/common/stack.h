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

#ifndef __STACK_H
#define __STACK_H

#include <execinfo.h>

namespace common {

class Stack
{
private:
    static const unsigned int MAX_SIZE = 64;

public:
    explicit Stack( size_t maxSize = MAX_SIZE )
    : strings_( nullptr )
    {
        size_ = backtrace( array_, std::min( maxSize, sizeof(array_) / sizeof(void *) ) );
    }

    ~Stack()
    {
        if ( strings_ )
            free( strings_ );
    }

    template< typename T >
    void Out( T &out )
    {
        if ( strings_ )
            free( strings_ );

        strings_ = backtrace_symbols( array_, size_ );

        for( size_t i = 0; i < size_; ++i )
            out << strings_[i] << std::endl;
    }

private:
    void *array_[ MAX_SIZE ];
    size_t size_;
    char **strings_;
};

} // namespace common

#endif

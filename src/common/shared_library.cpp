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

#include <dlfcn.h>
#include "shared_library.h"
#include "log.h"

namespace common {

SharedLibrary::SharedLibrary()
: handle_( nullptr )
{}

SharedLibrary::~SharedLibrary()
{
    Close();
}

bool SharedLibrary::Load( const char *fileName )
{
    handle_ = dlopen( fileName, RTLD_LAZY );
    if ( !handle_ )
    {
        PLOG_ERR( "SharedLibrary: dlopen failed: " << dlerror() );
        return false;
    }
    return true;
}

void SharedLibrary::Close()
{
    if ( handle_ )
        dlclose( handle_ );
}

void *SharedLibrary::GetFunction( const char *function )
{
    return dlsym( handle_, function );
}

} // namespace common

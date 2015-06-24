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

#ifndef __HISTORY_H
#define __HISTORY_H

#include <string>

namespace common {

// NB: increment this value, if IHistory changes
static const int HISTORY_VERSION = 1;

// NB: throws exception on error
struct IHistory
{
    virtual ~IHistory() {}
    virtual void Initialize( const std::string &configPath ) = 0;
    virtual void Shutdown() = 0;

    virtual void Put( const std::string &key, const std::string &value ) = 0;
    virtual void Delete( const std::string &key ) = 0;

    typedef void (*GetCallback)( const std::string &key, const std::string &value );
    virtual void GetAll( GetCallback callback ) = 0;
};

} // namespace common


extern "C" common::IHistory *CreateHistory( int interfaceVersion );
extern "C" void DestroyHistory( const common::IHistory * );

#endif

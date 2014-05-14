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

#include "dbmemory.h"
#include "common/log.h"


namespace masterdb {

bool DbInMemory::Put( const std::string &key, const std::string &value )
{
    return idToString_.insert( PairType( key, value ) ).second;
}

bool DbInMemory::Delete( const std::string &key )
{
    return idToString_.erase( key ) > 0;
}

bool DbInMemory::Get( std::string &jobs )
{
    SSTable::const_iterator it = idToString_.begin();
    for( ; it != idToString_.end(); ++it )
    {
        jobs += it->first + '\n' + it->second + '\n';
    }
    return true;
}

} // namespace masterdb

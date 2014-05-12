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

#ifndef __DB_MEMORY_H
#define __DB_MEMORY_H

#include <map>
#include <utility> // pair
#include "dbaccess.h"


namespace masterdb {

class DbInMemory : public IDAO
{
typedef std::map< int64_t, std::string > IdToString;
typedef std::pair< int64_t, std::string > PairType;

public:
    // IDAO
    virtual bool Put( int64_t key, const std::string &value );
    virtual bool Get( int64_t key, std::string &value );
    virtual bool Delete( int64_t key );

private:
    IdToString idToString_;
};

} // namespace masterdb

#endif

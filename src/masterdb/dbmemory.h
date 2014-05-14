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
typedef std::map< std::string, std::string > SSTable;
typedef std::pair< std::string, std::string > PairType;

public:
    // IDAO
    virtual bool Put( const std::string &key, const std::string &value );
    virtual bool Delete( const std::string &key );
    virtual bool Get( std::string &jobs );

private:
    SSTable idToString_;
};

} // namespace masterdb

#endif

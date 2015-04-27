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

#ifndef __DB_ACTION_H
#define __DB_ACTION_H

#include "dbrequest.h"


namespace masterdb {

class IDbAction
{
public:
    virtual ~IDbAction() {}
    virtual bool Execute( const DbRequest &request, std::string &response ) = 0;
};

class DbPut : public IDbAction
{
public:
    virtual bool Execute( const DbRequest &request, std::string &response );
};

class DbDelete : public IDbAction
{
public:
    virtual bool Execute( const DbRequest &request, std::string &response );
};

class DbGet : public IDbAction
{
public:
    virtual bool Execute( const DbRequest &request, std::string &response );
};

class DbActionCreator
{
public:
    virtual IDbAction *Create( const std::string &taskType )
    {
        if ( taskType == "PUT" )
            return new DbPut();
        if ( taskType == "DELETE" )
            return new DbDelete();
        if ( taskType == "GET" )
            return new DbGet();
        return nullptr;
    }
};

} // namespace masterdb

#endif

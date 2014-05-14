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

#include "dbaction.h"
#include "dbaccess.h"
#include "common/service_locator.h"


namespace masterdb {

bool DbPut::Execute( const DbRequest &request, std::string &response )
{
    const DbRequest::ArgList &args = request.GetArgs();
    if ( args.size() < 1 )
    {
        PLOG_ERR( "DbPut::Execute: missing key" );
        return false;
    }

    const std::string &key = *args.begin();
    if ( key.empty() )
    {
        PLOG_ERR( "DbPut::Execute: empty key" );
        return false;
    }

    IDAO *dbClient = common::ServiceLocator::Instance().Get< IDAO >();
    return dbClient->Put( key, request.GetData() );
}

bool DbDelete::Execute( const DbRequest &request, std::string &response )
{
    const DbRequest::ArgList &args = request.GetArgs();
    if ( args.size() < 1 )
    {
        PLOG_ERR( "DbPut::Execute: missing key" );
        return false;
    }

    const std::string &key = *args.begin();
    if ( key.empty() )
    {
        PLOG_ERR( "DbPut::Execute: empty key" );
        return false;
    }

    IDAO *dbClient = common::ServiceLocator::Instance().Get< IDAO >();
    return dbClient->Delete( key );
}

bool DbGet::Execute( const DbRequest &request, std::string &response )
{
    IDAO *dbClient = common::ServiceLocator::Instance().Get< IDAO >();
    return dbClient->Get( response );
}

} // namespace masterdb

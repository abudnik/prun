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

#include <stdexcept>
#include "dblevel.h"
#include "common/config.h"
#include "common/log.h"


namespace masterdb {

void DbLevel::Initialize( const std::string &exeDir )
{
    common::Config &cfg = common::Config::Instance();
    std::string dbDir = cfg.Get<std::string>( "db_path" );
    if ( dbDir.empty() || dbDir[0] != '/' )
    {
        dbDir = exeDir + '/' + dbDir;
    }

    leveldb::DB *db = nullptr;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open( options, dbDir.c_str(), &db );
    if ( !status.ok() )
        throw std::logic_error( status.ToString() );

    pDB.reset( db );
}

bool DbLevel::Put( const std::string &key, const std::string &value )
{
    leveldb::Status status = pDB->Put( leveldb::WriteOptions(), key, value );
    if ( !status.ok() )
    {
        PLOG_WRN( "DbLevel::Put: " << status.ToString() );
        return false;
    }

    return true;
}

bool DbLevel::Delete( const std::string &key )
{
    leveldb::Status status = pDB->Delete( leveldb::WriteOptions(), key );
    if ( !status.ok() )
    {
        PLOG_WRN( "DbLevel::Delete: " << status.ToString() );
        return false;
    }

    return true;
}

bool DbLevel::Get( std::string &jobs )
{
    std::unique_ptr< leveldb::Iterator > it( pDB->NewIterator( leveldb::ReadOptions() ) );
    for( it->SeekToFirst(); it->Valid(); it->Next() )
    {
        jobs += it->key().ToString() + '\n' + it->value().ToString() + '\n';
    }

    if ( !it->status().ok() )
    {
        PLOG_WRN( "DbLevel::Get: " << it->status().ToString() );
        return false;
    }

    return true;
}

} // namespace masterdb

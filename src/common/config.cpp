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

#define BOOST_SPIRIT_THREADSAFE

#include <stdint.h> // boost/atomic/atomic.hpp:202:16: error: ‘uintptr_t’ was not declared in this scope
#include <boost/property_tree/json_parser.hpp>
#include <fstream>
#include "config.h"
#include "log.h"

using namespace common;

const char Config::defaultCfgName[] = "main.cfg";


bool Config::ParseConfig( const char *cfgPath, const char *cfgName )
{
    configPath_ = cfgPath;
    if ( !configPath_.empty() )
        configPath_ += '/';
    configPath_ += cfgName;

    std::ifstream file( configPath_.c_str() );
    if ( !file.is_open() )
    {
        PLOG_ERR( "Config::ParseConfig: couldn't open " << configPath_ );
        return false;
    }
    try
    {
        boost::property_tree::read_json( file, ptree_ );
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "Config::ParseConfig: " << e.what() );
        return false;
    }

    return true;
}

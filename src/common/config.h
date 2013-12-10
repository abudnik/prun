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

#ifndef __CONFIG_H
#define __CONFIG_H

#include <boost/property_tree/ptree.hpp>


namespace common {

class Config
{
public:
    template<typename T>
    T Get( const char *key )
    {
        return ptree_.get<T>( key );
    }

    bool ParseConfig( const char *cfgPath, const char *cfgName );

    static Config &Instance()
    {
        static Config instance_;
        return instance_;
    }

private:
    std::string configPath_;
    boost::property_tree::ptree ptree_;
};

} // namespace common

#endif

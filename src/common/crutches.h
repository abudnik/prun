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

#ifndef __CRUTCHES_H
#define __CRUTCHES_H

namespace common {

//////// crutches for boost library ////////

template< typename T >
std::string PathToString( const T &path );

template<>
std::string PathToString( const std::string &path )
{
    return path;
}

template<>
std::string PathToString( const boost::filesystem::path &path )
{
    return path.string();
}

} // namespace common

#endif


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

#ifndef __SERVICE_LOCATOR_H
#define __SERVICE_LOCATOR_H

#include <typeinfo>
#include <unordered_map>
#include <stdexcept>
#include "common/log.h"

namespace common {

class ServiceLocator
{
    typedef std::unordered_map< const std::type_info *, void * > ServiceContainer;

public:
    template< typename T >
    bool Register( T *service )
    {
        try
        {
            const std::type_info &type = typeid( T );
            void *pService = reinterpret_cast<void *>( service );

            if ( !services_.insert( std::make_pair( &type, pService ) ).second )
            {
                PLOG_WRN( "ServiceLocator::Register: service with type=" << type.name() <<
                          " already registered" );
                return false;
            }
        }
        catch( std::bad_typeid &ex )
        {
            PLOG_ERR( "ServiceLocator::Register: bad_typeid caught: " << ex.what() );
            return false;
        }
        return true;
    }

    template< typename T >
    bool Unregister( T *service )
    {
        try
        {
            const std::type_info &type = typeid( T );

            auto it = services_.find( &type );
            if ( it != services_.end() )
            {
                services_.erase( it );
            }
            else
            {
                PLOG_WRN( "ServiceLocator::Register: service with type=" << type.name() <<
                          " is not registered" );
                return false;
            }
        }
        catch( std::bad_typeid &ex )
        {
            PLOG_ERR( "ServiceLocator::Register: bad_typeid caught: " << ex.what() );
            return false;
        }
        return true;
    }

    void UnregisterAll()
    {
        services_.clear();
    }

    template< typename T >
    T *Get() // throw( std::bad_typeid, ServiceNotFoundException )
    {
        try
        {
            const std::type_info &type = typeid( T );

            auto it = services_.find( &type );
            if ( it != services_.end() )
            {
                return reinterpret_cast<T*>( it->second );
            }

            throw std::logic_error( std::string( "Service not found: " ) + type.name() );
        }
        catch( std::bad_typeid &ex )
        {
            PLOG_ERR( "ServiceLocator::Get: bad_typeid caught: " << ex.what() );
            throw;
        }

        return nullptr;
    }

    static ServiceLocator &Instance()
    {
        static ServiceLocator instance_;
        return instance_;
    }

private:
    ServiceContainer services_;
};

template< typename T >
inline T *GetService()
{
    return ServiceLocator::Instance().Get< T >();
}

} // namespace common

#endif

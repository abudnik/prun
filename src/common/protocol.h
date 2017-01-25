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

#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include <string>
#include <set>
#include <list>
#include <stdint.h>
#include <boost/property_tree/ptree.hpp>
#include "log.h"

namespace common {

class Marshaller
{
public:
    typedef boost::property_tree::ptree Properties;

public:
    template< typename T >
    Marshaller &operator ()( const char *name, T var )
    {
        ptree_.put( name, var );
        return *this;
    }

    Marshaller &operator ()( const char *name, const std::string &var )
    {
        ptree_.put( name, var );
        return *this;
    }

    template< typename T >
    Marshaller &operator ()( const char *name, const std::set<T> &var )
    {
        Properties child, element;
        for( const auto &v : var )
        {
            element.put_value( v );
            child.push_back( std::make_pair( "", element ) );
        }
        ptree_.add_child( name, child );
        return *this;
    }

    const Properties &GetProperties() const { return ptree_; }

private:
    Properties ptree_;
};

class Demarshaller
{
public:
    typedef boost::property_tree::ptree Properties;

public:
    template< typename T >
    Demarshaller &operator ()( const char *name, T &var )
    {
        try
        {
            var = ptree_.get<T>( name );
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "Demarshaller: " << e.what() );
            throw;
        }
        return *this;
    }

    template< typename T >
    Demarshaller &operator ()( const char *name, std::set<T> &var )
    {
        try
        {
            for( const Properties::value_type &v: ptree_.get_child( name ) )
            {
                var.insert( v.second.get_value< T >() );
            }
        }
        catch( std::exception &e )
        {
            PLOG_ERR( "Demarshaller: " << e.what() );
            throw;
        }
        return *this;
    }

    Properties &GetProperties() { return ptree_; }

protected:
    Properties ptree_;
};

class Protocol
{
public:
    virtual ~Protocol() {}

    virtual bool Serialize( std::string &msg, const char *method, const Marshaller &marshaller ) = 0;

    virtual bool ParseBody( const std::string &msg, Demarshaller::Properties &ptree ) = 0;

    // commands section
    virtual bool SendCommand( std::string &msg, const std::string &masterId, const std::string &command,
                              const std::list< std::pair< std::string, std::string > > &params ) = 0;

    // internals
    virtual bool ParseMsgType( const std::string &msg, std::string &type ) = 0;

    static bool ParseMsg( const std::string &msg, std::string &protocol, int &version,
                          std::string &header, std::string &body );

protected:
    std::string CreateHeader( const std::string &msg ) const;

private:
    virtual const char *GetProtocolType() const = 0;
    virtual const char *GetProtocolVersion() const = 0;
};

class ProtocolJson : public Protocol
{
public:
    virtual bool Serialize( std::string &msg, const char *method, const Marshaller &marshaller );

    virtual bool ParseBody( const std::string &msg, Demarshaller::Properties &ptree );

    virtual bool SendCommand( std::string &msg, const std::string &masterId, const std::string &command,
                              const std::list< std::pair< std::string, std::string > > &params );

    virtual bool ParseMsgType( const std::string &msg, std::string &type );

private:
    virtual const char *GetProtocolType() const { return "json"; }
    virtual const char *GetProtocolVersion() const { return "1"; }
};

class ProtocolCreator
{
public:
    Protocol *Create( const std::string &protocol, int version )
    {
        if ( protocol == "json" )
            return new ProtocolJson();
        return nullptr;
    }
};

} // namespace common

#endif

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

#ifndef __COMMAND_H
#define __COMMAND_H

#include <sstream>
#include <list>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>

namespace master {

class Command
{
public:
    typedef std::pair< std::string, std::string > PairType;
    typedef std::list< PairType > Params;

public:
    Command( const std::string &command )
    : command_( command )
    {}
    virtual ~Command() {}

    void SetParam( const std::string &key, const std::string &value )
    {
        params_.push_back( PairType( key, value ) );
    }

    template< typename T >
    void SetParam( const std::string &key, const T &value )
    {
        std::ostringstream ss;
        ss << value;
        params_.push_back( PairType( key, ss.str() ) );
    }

    const std::string &GetCommand() const { return command_; }
    Params &GetAllParams() { return params_; }
    const Params &GetAllParams() const { return params_; }

    template< typename T >
    void SetCallback( T &obj, void (T::*f)( int errCode, const std::string &hostIP ) )
    {
        callback_ = boost::bind( f, obj, _1, _2 );
    }

    void OnExec( int errCode, const std::string &hostIP )
    {
        OnCompletion( errCode, hostIP );
        if ( callback_ )
            callback_( errCode, hostIP );
    }

    virtual int GetRepeatDelay() const = 0;

private:
    virtual void OnCompletion( int errCode, const std::string &hostIP ) = 0;

protected:
    std::string command_;
    Params params_;

private:
    boost::function< void (int, const std::string &) > callback_;
};

typedef boost::shared_ptr< Command > CommandPtr;

} // namespace master

#endif

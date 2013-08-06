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
#ifndef __HELPER_H
#define __HELPER_H

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>

namespace python_server {

class Semaphore
{
public:
	Semaphore( unsigned int v )
	: count_( v )
	{}

	void Notify()
	{
		boost::mutex::scoped_lock lock( mutex_ );
		++count_;
		condition_.notify_one();
	}

	void Wait()
	{
		boost::mutex::scoped_lock lock( mutex_ );
		while( !count_ )
			condition_.wait( lock );
		--count_;
	}

private:
	boost::mutex mutex_;
	boost::condition_variable condition_;
	unsigned int count_;
};

class SyncTimer
{
public:
	void StopWaiting()
	{
		boost::mutex::scoped_lock lock( mutex_ );
		condition_.notify_one();
	}

	bool Wait( int millisec )
	{
		boost::mutex::scoped_lock lock( mutex_ );
        const boost::system_time timeout = boost::get_system_time() + boost::posix_time::milliseconds( millisec );
        return !condition_.timed_wait( lock, timeout );
	}

private:
	boost::mutex mutex_;
	boost::condition_variable condition_;
};

template< typename T >
void EncodeBase64( const T *data, std::size_t size, std::string &out )
{
  using namespace boost::archive::iterators;
  typedef base64_from_binary< transform_width< char*, 6, 8 > > translate_out;
  out.append( translate_out( data ), translate_out( data + size ) );
}

template< typename Container >
void DecodeBase64( std::string &data, Container &out )
{
  using namespace boost::archive::iterators;
  typedef transform_width< binary_from_base64< std::string::const_iterator >, 8, 6 > translate_in;

  std::size_t padding = data.size() % 4;
  data.append( padding, std::string::value_type( '=' ) );

  std::copy( translate_in( data.begin() ), translate_in( data.end() - padding ), std::back_inserter( out ) );
}

} // namespace python_server

#endif

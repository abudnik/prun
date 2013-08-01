#ifndef __OBSERVER_H
#define __OBSERVER_H

#include <boost/thread/mutex.hpp>
#include <set>

namespace python_server {

class Observer
{
public:
    virtual void NotifyObserver( int event ) = 0;
    virtual ~Observer() {}
};

template< bool multithreaded >
class Observable;

template<>
class Observable< false >
{
typedef std::set< Observer * > Container;

public:
	void Subscribe( Observer *observer )
	{
		observers_.insert( observer );
	}

	void Unsubscribe( Observer *observer )
	{
		observers_.erase( observer );
	}

	void NotifyAll( int event )
	{
		Container::iterator it = observers_.begin();
		for( ; it != observers_.end(); ++it )
		{
			(*it)->NotifyObserver( event );
		}
	}

private:
    Container observers_;
};

template<>
class Observable< true >
{
typedef std::set< Observer * > Container;

public:
	void Subscribe( Observer *observer )
	{
		boost::mutex::scoped_lock scoped_lock( mut_ );
		observers_.insert( observer );
	}

	void Unsubscribe( Observer *observer )
	{
		boost::mutex::scoped_lock scoped_lock( mut_ );
		observers_.erase( observer );
	}

	void NotifyAll( int event )
	{
		boost::mutex::scoped_lock scoped_lock( mut_ );
		Container::iterator it = observers_.begin();
		for( ; it != observers_.end(); ++it )
		{
			(*it)->NotifyObserver( event );
		}
	}

private:
    Container observers_;
	boost::mutex mut_;
};

} // namespace python_server

#endif

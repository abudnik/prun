#ifndef __OBSERVER_H
#define __OBSERVER_H

#include <set>
#include <boost/thread/locks.hpp>  
#include <boost/thread/shared_mutex.hpp> 

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

	void NotifyAll( int event = 0 )
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
        boost::upgrade_lock< boost::shared_mutex > lock( mut_ );
        boost::upgrade_to_unique_lock< boost::shared_mutex > uniqueLock( lock );
		observers_.insert( observer );
	}

	void Unsubscribe( Observer *observer )
	{
        boost::upgrade_lock< boost::shared_mutex > lock( mut_ );
        boost::upgrade_to_unique_lock< boost::shared_mutex > uniqueLock( lock );
		observers_.erase( observer );
	}

	void NotifyAll( int event = 0 )
	{
		boost::shared_lock< boost::shared_mutex > lock( mut_ );
		Container::iterator it = observers_.begin();
		for( ; it != observers_.end(); ++it )
		{
			(*it)->NotifyObserver( event );
		}
	}

private:
    Container observers_;
	boost::shared_mutex mut_;
};

} // namespace python_server

#endif

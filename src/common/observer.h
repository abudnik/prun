#ifndef __OBSERVER_H
#define __OBSERVER_H

#include <set>
#include <map>
#include <boost/thread/locks.hpp>  
#include <boost/thread/shared_mutex.hpp> 

namespace common {

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
    typedef std::set<Observer *> Container;
    typedef std::map< int, Container > EventToContainer;

public:
    void Subscribe( Observer *observer, int event = 0 )
    {
        observers_[ event ].insert( observer );
    }

    void Unsubscribe( Observer *observer, int event = 0 )
    {
        EventToContainer::iterator it = observers_.find( event );
        if ( it != observers_.end() )
        {
            it->second.erase( observer );
        }
    }

    void NotifyAll( int event = 0 )
    {
        EventToContainer::iterator it = observers_.find( event );
        if ( it == observers_.end() )
            return;
        
        Container::iterator it_ob = it->second.begin();
        for( ; it_ob != it->second.end(); ++it_ob )
        {
            Observer *observer = *it_ob;
            observer->NotifyObserver( event );
        }
    }

private:
    EventToContainer observers_;
};

template<>
class Observable< true >
{
    typedef std::set<Observer *> Container;
    typedef std::map< int, Container > EventToContainer;

public:
    void Subscribe( Observer *observer, int event = 0 )
    {
        boost::upgrade_lock< boost::shared_mutex > lock( mut_ );
        boost::upgrade_to_unique_lock< boost::shared_mutex > uniqueLock( lock );
        observers_[ event ].insert( observer );
    }

    void Unsubscribe( Observer *observer, int event = 0 )
    {
        boost::upgrade_lock< boost::shared_mutex > lock( mut_ );
        boost::upgrade_to_unique_lock< boost::shared_mutex > uniqueLock( lock );
        EventToContainer::iterator it = observers_.find( event );
        if ( it != observers_.end() )
        {
            it->second.erase( observer );
        }
    }

    void NotifyAll( int event = 0 )
    {
        boost::shared_lock< boost::shared_mutex > lock( mut_ );
        EventToContainer::iterator it = observers_.find( event );
        if ( it == observers_.end() )
            return;
        
        Container::iterator it_ob = it->second.begin();
        for( ; it_ob != it->second.end(); ++it_ob )
        {
            Observer *observer = *it_ob;
            observer->NotifyObserver( event );
        }
    }

private:
    EventToContainer observers_;
    boost::shared_mutex mut_;
};

} // namespace common

#endif

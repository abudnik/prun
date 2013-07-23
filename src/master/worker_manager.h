#include <list>
#include "worker.h"

namespace master {

class WorkerManager
{
public:
    template< template< class, class > class List >
    void Initialize( const List< std::string, std::allocator< std::string > > &hosts )
    {
        typename List< std::string, std::allocator< std::string > >::const_iterator it = hosts.begin();
        for( ; it != hosts.end(); ++it )
        {
            workers_.AddWorker( new Worker( (*it).c_str() ) );
        }
    }

    void Shutdown()
    {
        workers_.Clear();
    }

    static WorkerManager &Instance()
    {
        static WorkerManager instance_;
        return instance_;
    }

private:
    WorkerList workers_;
};

bool ReadHosts( const char *filePath, std::list< std::string > &hosts );

} // namespace master

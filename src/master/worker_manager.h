#include <list>
#include "worker.h"

namespace master {

class WorkerManager
{
public:
    template< class Container >
    void Initialize( const Container &hosts )
    {
        typename Container::const_iterator it = hosts.begin();
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

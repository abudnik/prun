#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/move/move.hpp>
#include <boost/filesystem.hpp>
#include <fstream>
#include "log.h"

using namespace std;
using namespace boost::interprocess;

namespace python_server {

class Pidfile
{
public:
    Pidfile( const char *filePath )
    : filePath_( filePath )
    {
        bool fileExists = boost::filesystem::exists( filePath );

        file_.open( filePath );

        file_lock f_lock( filePath );
        if ( !f_lock.try_lock() )
        {
            PS_LOG( "can't exclusively lock pid_file=" << filePath_ );
            exit( 1 );
        }

        f_lock_ = boost::move( f_lock );

        file_ << getpid();
        file_.flush();

        afterFail_ = fileExists;
        if ( afterFail_ )
        {
            PS_LOG( "previous process terminated abnormally" );
        }
    }

    ~Pidfile()
    {
        try
        {
            f_lock_.unlock();
            file_.close();
            boost::filesystem::remove( filePath_ );
        }
        catch(...)
        {
            PS_LOG( "exception in ~Pidfile()" );
        }
    }

    bool AfterFail() const { return afterFail_; }

private:
    bool afterFail_;
    std::string filePath_;
    file_lock f_lock_;
    ofstream file_;
};

} // namespace python_server

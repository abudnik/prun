#include <boost/property_tree/json_parser.hpp>
#include <fstream>
#include "log.h"

using namespace std;


namespace python_server {

class Config
{
static const char defaultCfgName[];

public:
    template<typename T>
    T Get( const char *key )
    {
        return ptree_.get<T>( key );
    }

    bool ParseConfig( const char *cfgName = defaultCfgName )
    {
        configName_ = cfgName;

        ifstream file( configName_.c_str() );
        if ( !file.is_open() )
        {
			PS_LOG( "Config::ParseConfig: couldn't open " << configName_ );
            return false;
        }
        try
        {
            boost::property_tree::read_json( file, ptree_ );
        }
		catch( std::exception &e )
		{
			PS_LOG( "Config::ParseConfig: " << e.what() );
            return false;
		}

        return true;
    }

    static Config &Instance()
    {
        static Config instance_;
        return instance_;
    }

private:
    string configName_;
	boost::property_tree::ptree ptree_;
};

} // namespace python_server

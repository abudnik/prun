#ifndef __CONFIG_H
#define __CONFIG_H

#include <boost/property_tree/json_parser.hpp>
#include <fstream>
#include "log.h"


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

    bool ParseConfig( const char *cfgPath = "", const char *cfgName = defaultCfgName )
    {
        configPath_ = cfgPath;
        if ( !configPath_.empty() )
            configPath_ += '/';
        configPath_ += cfgName;

        std::ifstream file( configPath_.c_str() );
        if ( !file.is_open() )
        {
			PS_LOG( "Config::ParseConfig: couldn't open " << configPath_ );
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
    std::string configPath_;
	boost::property_tree::ptree ptree_;
};

} // namespace python_server

#endif

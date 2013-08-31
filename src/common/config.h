#ifndef __CONFIG_H
#define __CONFIG_H

#include <boost/property_tree/ptree.hpp>


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

    bool ParseConfig( const char *cfgPath = "", const char *cfgName = defaultCfgName );

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

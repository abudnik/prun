#ifndef __COMMAND_H
#define __COMMAND_H

#include <sstream>
#include <list>
#include <boost/shared_ptr.hpp>

namespace master {

class Command
{
public:
    typedef std::pair< std::string, std::string > PairType;
    typedef std::list< PairType > Params;

public:
    virtual ~Command() {}

    void SetParam( const std::string &key, const std::string &value )
    {
        params_.push_back( PairType( key, value ) );
    }

    template< typename T >
    void SetParam( const std::string &key, const T &value )
    {
        std::ostringstream ss;
        ss << value;
        params_.push_back( PairType( key, ss.str() ) );
    }

    const std::string &GetCommand() const { return command_; }
    Params &GetAllParams() { return params_; }
    const Params &GetAllParams() const { return params_; }

protected:
    std::string command_;
    Params params_;
};

typedef boost::shared_ptr< Command > CommandPtr;

} // namespace master

#endif

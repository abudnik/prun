#ifndef __HISTORY_H
#define __HISTORY_H

#include <string>

namespace common {

// NB: increment this value, if IHistory changes
static const int HISTORY_VERSION = 1;

// NB: throws exception on error
struct IHistory
{
    virtual ~IHistory() {}
    virtual void Initialize( const std::string &configPath ) = 0;
    virtual void Shutdown() = 0;

    virtual void Put( const std::string &key, const std::string &value ) = 0;
    virtual void Delete( const std::string &key ) = 0;

    typedef void (*GetCallback)( const std::string &key, const std::string &value );
    virtual void GetAll( GetCallback callback ) = 0;
};

} // namespace common


extern "C" common::IHistory *CreateHistory( int interfaceVersion );
extern "C" void DestroyHistory( const common::IHistory * );

#endif

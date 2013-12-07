#include "uuid.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>

namespace common {

std::string GenerateUUID()
{
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    return boost::lexical_cast< std::string >( uuid );
}

} // namespace common

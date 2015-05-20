/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2013 Andrey Budnik <budnik27@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

===========================================================================
*/

#ifndef __USER_COMMAND_H
#define __USER_COMMAND_H

#include <fstream>
#include <stdint.h> // int64_t
#include "job.h"

namespace master {

class UserCommand
{
public:
    bool Run( const std::string &filePath, const std::string &jobAlias, std::string &result );
    bool Stop( int64_t jobId );
    bool StopGroup( int64_t groupId );
    bool StopNamed( const std::string &name );
    bool StopAll();
    bool StopPreviousJobs();
    bool AddWorkerHost( const std::string &groupName, const std::string &host );
    bool DeleteHost( const std::string &host );
    bool AddGroup( const std::string &filePath, const std::string &fileName );
    bool DeleteGroup( const std::string &group );
    bool Info( int64_t jobId, std::string &result );
    bool GetStatistics( std::string &result );
    bool GetAllJobInfo( std::string &result );
    bool GetWorkersStatistics( std::string &result );

private:
    bool RunJob( std::ifstream &file, const std::string &jobAlias, std::string &result ) const;
    bool RunMetaJob( std::ifstream &file, std::string &result ) const;
    void PrintJobInfo( const JobPtr &job, std::string &result ) const;
};

} // namespace master

#endif

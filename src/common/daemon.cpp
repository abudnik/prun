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

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <cerrno>
#include <csignal>
#include <iostream>
#include <sstream>
#include "daemon.h"

namespace common {

int StartAsDaemon()
{
    pid_t parpid, sid;

    // Fork the process and have the parent exit. If the process was started
    // from a shell, this returns control to the user. Forking a new process is
    // also a prerequisite for the subsequent call to setsid().
    parpid = fork();
    if ( parpid < 0 )
    {
        std::cout << "StartAsDaemon: fork() failed: " << strerror(errno) << std::endl;
        exit( parpid );
    }
    else
    if ( parpid > 0 )
    {
        exit( 0 );
    }

    // Make the process a new session leader. This detaches it from the
    // terminal.
    sid = setsid();
    if ( sid < 0 )
    {
        std::cout << "StartAsDaemon: setsid() failed: " << strerror(errno) << std::endl;
        exit( 1 );
    }

    // A process inherits its working directory from its parent. This could be
    // on a mounted filesystem, which means that the running daemon would
    // prevent this filesystem from being unmounted. Changing to the root
    // directory avoids this problem.
    chdir("/");

    // The file mode creation mask is also inherited from the parent process.
    // We don't want to restrict the permissions on files created by the
    // daemon, so the mask is cleared.
    umask(0);

    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);

    return sid;
}

int StopDaemon( const char *procName )
{
    std::ostringstream command;
    command << "pkill " << procName;

    return system( command.str().c_str() );
}

} // namespace common

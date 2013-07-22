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

namespace python_server {

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
	char line[256] = { '\0' };

	std::ostringstream command;
	command << "pidof -s -o " << getpid() << " " << procName;

	FILE *cmd = popen( command.str().c_str(), "r" );
	fgets( line, sizeof(line), cmd );
	pclose( cmd );

	if ( !strlen( line ) )
	{
		std::cout << "can't get pid of " << procName << ": " << strerror(errno) << std::endl;
		exit( 1 );
	}

	pid_t pid = strtoul( line, NULL, 10 );

	std::cout << "sending SIGTERM to " << pid << std::endl;
	return kill( pid, SIGTERM );
}

} // namespace python_server
#ifndef __DAEMON_H
#define __DAEMON_H

namespace python_server {

int StartAsDaemon();
int StopDaemon( const char *procName );

} // namespace python_server

#endif

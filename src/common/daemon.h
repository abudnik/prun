#ifndef __DAEMON_H
#define __DAEMON_H

namespace common {

int StartAsDaemon();
int StopDaemon( const char *procName );

} // namespace common

#endif

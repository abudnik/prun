#!/bin/sh

die () {
    echo "ERROR: $1. Aborting!"
    exit 1
}

echo "Welcome to the prun master service installer"
echo "This script will help you easily set up a running prun master server

"

if [ `id -u` != "0" ] ; then
	echo "You must run this script as root. Sorry!"
	exit 1
fi

#read master jobs directory
_MASTER_JOBS_DIR="/var/lib/pmaster"
read -p "Please select the master jobs directory [$_MASTER_JOBS_DIR] " MASTER_JOBS_DIR
if [ -z "$MASTER_JOBS_DIR" ] ; then
	MASTER_JOBS_DIR=$_MASTER_JOBS_DIR
	echo "Selected default - $MASTER_JOBS_DIR"
fi
mkdir -p $MASTER_JOBS_DIR || die "Could not create master jobs directory"
MASTER_JOBS_DIR=$MASTER_JOBS_DIR"/jobs"
cp -rf "jobs" $MASTER_JOBS_DIR || die "Could not copy jobs directory"

#replace pidfile path in config file
PIDFILE="/var/run/pmaster.pid"
TMP_CONFIG=`mktemp`

cp -f "master.cfg" $TMP_CONFIG || die "could not copy 'master.cfg' to $TMP_CONFIG"
sed -i "s|master.pid|$PIDFILE|g" $TMP_CONFIG
sed -i "s|\"jobs\"|\"$MASTER_JOBS_DIR\"|g" $TMP_CONFIG

#read master config file
_MASTER_CONFIG_FILE="/etc/pmaster/master.cfg"
read -p "Please select the master config file name [$_MASTER_CONFIG_FILE] " MASTER_CONFIG_FILE
if [ -z "$MASTER_CONFIG_FILE" ] ; then
	MASTER_CONFIG_FILE=$_MASTER_CONFIG_FILE
	echo "Selected default - $MASTER_CONFIG_FILE"
fi
#try and create it
MASTER_CONFIG_DIR=`dirname "$MASTER_CONFIG_FILE"`
mkdir -p $MASTER_CONFIG_DIR || die "Could not create master config directory"
cp -f $TMP_CONFIG $MASTER_CONFIG_FILE || die "Could not copy configuration file"
cp -f "hosts" $MASTER_CONFIG_DIR"/hosts" || die "Could not copy 'hosts' file"
cp -f "hosts_group1" $MASTER_CONFIG_DIR"/hosts_group1" || die "Could not copy 'hosts_group' file"

#read master executable directory
_MASTER_EXE_DIR="/usr/bin"
read -p "Please select the master executable directory [$_MASTER_EXE_DIR] " MASTER_EXE_DIR
if [ -z "$MASTER_EXE_DIR" ] ; then
	MASTER_EXE_DIR=$_MASTER_EXE_DIR
	echo "Selected default - $MASTER_EXE_DIR"
fi
mkdir -p $MASTER_EXE_DIR || die "Could not create master executable directory"
cp -f "pmaster" $MASTER_EXE_DIR || die "Could not copy executable file"

#get master executable path
MASTER_EXECUTABLE=`which pmaster`
if [ ! -f "$MASTER_EXECUTABLE" ] ; then
	echo "Could not find master executable"
	exit 1
fi

INIT_TPL_FILE="utils/master_init_script.tpl"
INIT_SCRIPT_DEST="/etc/init.d/pmaster"

MASTER_INIT_HEADER=\
"#/bin/sh\n
#Configurations injected by install_master below....\n\n
EXEC=\"$MASTER_EXECUTABLE\"\n
CONF=\"$MASTER_CONFIG_FILE\"\n\n
###############\n\n"

MASTER_CHKCONFIG_INFO=\
"# REDHAT chkconfig header\n\n
# chkconfig: - 58 74\n
# description: pmaster is the prun master daemon.\n
### BEGIN INIT INFO\n
# Provides: pmaster\n
# Required-Start: $network $local_fs $remote_fs\n
# Required-Stop: $network $local_fs $remote_fs\n
# Default-Start: 2 3 4 5\n
# Default-Stop: 0 1 6\n
# Should-Start: $syslog $named\n
# Should-Stop: $syslog $named\n
# Short-Description: start and stop pmaster\n
# Description: Prun master daemon\n
### END INIT INFO\n\n"

#Generate config file from the default config file as template
#changing only the stuff we're controlling from this script
echo "## Generated by install_master.sh ##" > $TMP_CONFIG

if [ -z `which chkconfig` ] ; then
	#combine the header and the template (which is actually a static footer)
	/bin/echo -e $MASTER_INIT_HEADER > $TMP_CONFIG && cat $INIT_TPL_FILE >> $TMP_CONFIG || die "Could not write init script to $TMP_CONFIG"
else
	#if we're a box with chkconfig on it we want to include info for chkconfig
	/bin/echo -e $MASTER_INIT_HEADER $MASTER_CHKCONFIG_INFO > $TMP_CONFIG && cat $INIT_TPL_FILE >> $TMP_CONFIG || die "Could not write init script to $TMP_CONFIG"
fi

#copy to /etc/init.d
cp -f $TMP_CONFIG $INIT_SCRIPT_DEST && chmod +x $INIT_SCRIPT_DEST || die "Could not copy master init script to  $INIT_SCRIPT_DEST"
echo "Copied $TMP_CONFIG => $INIT_SCRIPT_DEST"

rm -f $TMP_CONFIG

#Install the service
echo "Installing service..."
if [ -z `which chkconfig` ] ; then 
	#if we're not a chkconfig box assume we're able to use update-rc.d
	update-rc.d pmaster defaults && echo "Success!"
else
	# we're chkconfig, so lets add to chkconfig and put in runlevel 345
	chkconfig --add pmaster && echo "Successfully added to chkconfig!"
	chkconfig --level 345 pmaster on && echo "Successfully added to runlevels 345!"
fi

/etc/init.d/pmaster start || die "Failed starting service..."

echo "Installation successful!"
exit 0


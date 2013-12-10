#!/bin/sh

die () {
    echo "ERROR: $1. Aborting!"
    exit 1
}

echo "Welcome to the prun master service installer"
echo "This script will help you easily set up a running prun master server

"

#if [ `id -u` != "0" ] ; then
#	echo "You must run this script as root. Sorry!"
#	exit 1
#fi

#read master config file
_MASTER_CONFIG_FILE="/etc/pmaster/master.cfg"
read -p "Please select the master config file name [$_MASTER_CONFIG_FILE] " MASTER_CONFIG_FILE
if [ !"$MASTER_CONFIG_FILE" ] ; then
	MASTER_CONFIG_FILE=$_MASTER_CONFIG_FILE
	echo "Selected default - $MASTER_CONFIG_FILE"
fi
#try and create it
#mkdir -p `dirname "$MASTER_CONFIG_FILE"` || die "Could not create master config directory"

#get master data directory
_MASTER_DATA_DIR="/var/lib/pmaster"
read -p "Please select the data directory for this instance [$_MASTER_DATA_DIR] " MASTER_DATA_DIR
if [ !"$MASTER_DATA_DIR" ] ; then
	MASTER_DATA_DIR=$_MASTER_DATA_DIR
	echo "Selected default - $MASTER_DATA_DIR"
fi
#mkdir -p $MASTER_DATA_DIR || die "Could not create master data directory"

#read master executable file
_MASTER_EXE_DIR="/usr/bin/pmaster"
read -p "Please select the master executable file name [$_MASTER_EXE_DIR] " MASTER_EXE_DIR
if [ !"$MASTER_EXE_DIR" ] ; then
	MASTER_EXE_DIR=$_MASTER_EXE_DIR
	echo "Selected default - $MASTER_EXE_DIR"
fi
#mkdir -p $MASTER_EXE_DIR || die "Could not create master executable directory"


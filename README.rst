Overview
--------

PRUN provides control over batch jobs and distributed computing resources.
It consists of a single master daemon and worker daemons installed across the
computer cluster. Master provides a queueing mechanism, scheduling, priority scheme,
failover and planning of cron jobs. Worker is responsible for running of batch jobs
and monitoring of their execution. Workers are fully controlled by a master node.
Prun can be used on Unix-like operating systems, including Linux and *BSD.

Where to find complete Prun documentation?
------------------------------------------

This README is just a fast "quick start" document. You can find more detailed
documentation at doc/README

Building
--------

Build requirements:

- cmake 2.6 (or higher)
- GCC 4.6 (or higher) or Clang
- boost 1.46 (or higher)

Additional requirements:

- python 2.6/3.x (admin tool written in Python)

Building debian packages::

> git clone https://github.com/abudnik/prun.git
> cd prun
> debuild -sa -j8
> ls ../prun*.deb

Building RPMs::

> git clone https://github.com/abudnik/prun.git
> mv prun prun-0.15  # add '-version' postfix
> mkdir -p rpmbuild/SOURCES
> tar cjf rpmbuild/SOURCES/prun-0.15.tar.bz2 prun-0.15
> rpmbuild -ba prun-0.15/prun-bf.spec
> ls rpmbuild/RPMS/*/*.rpm

Building runtime from sources::

> git clone https://github.com/abudnik/prun.git
> cd prun
> cmake -DRelease=true .  # skipping Release var disables compiler optimizations
> make -j8

Optional external dependencies
---------------------

Master node can serialize its jobs state to external key-value database. After
master's node restart, master restores all previously submitted and not yet executed
jobs at node startup.
There are multiple bindings to different databases: LevelDB_, Cassandra_, Elliptics_.

.. _LevelDB: https://github.com/abudnik/prun-leveldb
.. _Cassandra: https://github.com/abudnik/prun-cassandra
.. _Elliptics: https://github.com/abudnik/prun-elliptics

Path to shared library (binding) and its config is set in master.cfg.

Running
-------

Running Worker in terminal::

> cd prun
> ./pworker

Use 'pworker --help' to display command line options.

Running Master in terminal::

> cd prun
> ./pmaster

Use 'pmaster --help' to display command line options.

Submitting 'Hello, World!' job::

> ./prun -c "run test.job"

Installation
------------

From debian package::

> dpkg -i prun-worker_0.15_amd64.deb

From RPM::

> rpm -ivh prun-worker-0.15-1.el7.centos.1.x86_64.rpm

Also it is possible to install prun worker and master using installation scripts
(like 'make install')::

> cd prun                    # cd to the directory containing prun
> utils/install_master.sh    # install Master
> utils/install_worker.sh    # install Worker

The script will ask you a few questions and will setup everything you need
to run Master/Worker properly as a background daemon that will start again on
system reboots.

You'll be able to stop and start Master/Worker using the script named
/etc/init.d/pmaster and /etc/init.d/pworker, accordingly.

Job submission
--------------

Here's a simple example of external sort task. Let's assume that we have
network-shared directory named 'data', which is read/write available from any
node in a cluster. So we need parallelize sorting of a big text file.

One solution is to sort a separate small pieces of a file and then merge them into
one big sorted file. Small pieces of a file are called chunks. We can submit a job
that sorts chunks from Master to Workers. Here's a simple shell script (see
jobs/example/sort_chunk.sh) that does it properly::

  echo "Sorting chunk process started"
  echo "taskId="$taskId", numTasks="$numTasks", jobId="$jobId

  filename="data/input.txt"
  outFile="data/$taskId"

  fileSize=`stat --printf="%s" $filename`
  partSize=`expr $fileSize / $numTasks`

  dd if=$filename bs=$partSize skip=$taskId count=1 | sort --buffer-size=$partSize"b" > $outFile
  errCode=${PIPESTATUS[0]}

  exit $errCode

For submitting a chunk sorting job, we should describe it in a .job file (see
jobs/sort_chunk.job), that is written in JSON format (see doc/README for more
detailed description)::

  {
      "script" : "example/sort_chunk.sh",
      "language" : "shell",
      "send_script" : true,
      "priority" : 4,
      "job_timeout" : 1800,
      "queue_timeout" : 300,
      "task_timeout" : 300,
      "max_failed_nodes" : 10,
      "num_execution" : 16,
      "max_cluster_instances" : -1,
      "max_worker_instances" : 1,
      "exclusive" : false,
      "no_reschedule" : false
  }

In a few words this job should be executed 16 times, using exactly one CPU of a
Worker node and should be done within 1800 seconds. It means that if we have
16 Worker nodes (computers/CPUs), each worker node will sort one of sixteen
chunks of the input big file. Even if we have only one worker, chunk sorting
job will be executed sixteen times.

After sorting chunks, this chunks could be merged together in one big output file.
Here's a simple shell script (see jobs/example/sort_merge.sh) which does
it properly::

  echo "Chunk merging process started"
  echo "taskId="$taskId", numTasks="$numTasks", jobId="$jobId

  chunks=`ls -d data/*[0-9]`
  outFile="data/output.txt"

  sort --buffer-size=33% -T "data" -m $chunks > $outFile
  errCode=$?

  exit $errCode

And merge job description (see jobs/sort_merge.job)::

  {
      "script" : "example/sort_merge.sh",
      "language" : "shell",
      "send_script" : true,
      "priority" : 4,
      "job_timeout" : 1800,
      "queue_timeout" : 1800,
      "task_timeout" : 1800,
      "max_failed_nodes" : 10,
      "num_execution" : 1,
      "max_cluster_instances" : -1,
      "max_worker_instances" : 1,
      "exclusive" : false,
      "no_reschedule" : false
  }

We want to run merging job strictly after completion of all chunk sorting jobs.
It is possible to describe job dependencies in a directed acyclic graph. Prun
takes that job dependencies from the .meta file. Here's a simple job dependency
between two jobs (see jobs/external_sort.meta)::

  {
      "graph" : [["sort_chunk.job", "sort_merge.job"]]
  }

Ok, we are almost done. We are having everything that is needed for sorting
the big file: running Workers across cluster nodes, one running Master process,
jobs and job descriptions, shared directory containing the input file
(data/input.txt). Lets submit job using command-line tool::

> cd prun                          # cd to the directory containing prun
> ./prun master_hostname           # run admin tool, connect to Master host
> run external_sort.meta           # submit a meta job

Cron job submission
-------------------

Next example is dumping mysql database at muliple hosts simultaneously.
Dumping planned at 3 a.m. at Sunday every week.

Firstly, we should create shell script, which does database dumping::

  mysqldump -uroot -pQWERTY -A > /home/nobody/dump/all-databases.sql

And save this script to master's jobs directory, e.g. at /home/nobody/jobs/dump.sh

Then we should create job description file of our cron job::

  {
    "script" : "/home/nobody/jobs/dump.sh",
    "language" : "shell",
    "send_script" : true,
    "priority" : 8,
    "job_timeout" : 3600,
    "queue_timeout" : 60,
    "task_timeout" : 3600,
    "max_failed_nodes" : -1,
    "num_execution" : -1,
    "max_cluster_instances" : -1,
    "max_worker_instances" : 1,
    "exclusive" : true,
    "no_reschedule" : true,
    "max_exec_at_worker" : 1,
    "exec_unit_type" : "host",
    "cron" : "* 3 * * 0",
    "name" : "weekly_dump"
  }

And save it to master's jobs directory, e.g. at /home/nobody/jobs/dump.job
This job will be started once at every available host at 3 a.m. every Sunday
after submitting it to the master (here master is running at localhost)::

  ./prun -c "run /home/nobody/jobs/dump.job" localhost

Stopping of any jobs by it's exclusive name performed by 'stop' command::

  ./prun -c "stop weekly_dump"

License
-------

The contents of this repository are made available to the public under the terms
of the Apache License, Version 2.0. For more information see LICENSE.txt

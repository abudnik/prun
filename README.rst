PRUN: Parallel task executor and job scheduler in a high-availability computing clusters
----------------------------------------------------------------------------------------

Prun is a an open-source cluster job scheduler and parallel task executor system.
Prun can be compiled and used on *nix-like operating systems, but currently
it has been tested only under Linux.

Like other full-featured batch systems, Prun provides a job queueing mechanism,
job scheduling, priority scheme, resource monitoring, and resource management.

Prun consists of Worker and Master components. Users submit their jobs to Master,
while Master places them into a queue, chooses when and where to run the jobs
based upon a policy, carefully monitors their progress and provides failover
facilities by detecting hardware/software faults, and immediately restarting the
application on another system without requiring administrative intervention.
Worker provides job execution and is fully controlled by a Master. Each Worker
instance, which is running across different cluster nodes, may start, monitor
and stop jobs assigned by a Master.

Where to find complete Prun documentation?
-------------------------------------------

This README is just a fast "quick start" document. You can find more detailed
documentation at doc/README

Building
--------

Build requirements:

- cmake 2.6 (or higher)
- GCC 3.x (or higher) or Clang
- boost 1.45 (or higher)

Additional requirements:

- python 2.6/3.x (for command-line admin tool & other purposes)

For running jobs written in Ruby, JavaScript or Java, requirements are as follows:

- ruby
- node.js
- java 1.6 (or higher)

Building runtime::

> cd ~/prun               # cd to the directory containing prun
> cmake -DRelease=true .  # skipping Release var disables compiler optimizations
> make                    # build executables

Running
-------

Running worker in terminal::

> ./pworker

Running worker as a daemon with paths to config file and special node directory::

> ./pworker --d --c /home/user/prun/worker.cfg --r /home/user/prun/node  # start
> ./pworker --s  # stop daemon

Use 'pworker --help' to display command line options.

Running master in terminal::

> ./pmaster

Running master as a daemon with path to config file::

> ./pmaster --d --c /home/user/prun/worker.cfg  # start
> ./pmaster --s  # stop daemon

Use 'pmaster --help' to display command line options.

Installation
------------

Job submitting
--------------

License
-------
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

#ifndef __STATISTICS_H
#define __STATISTICS_H

#include "scheduler.h"
#include "cron_manager.h"

namespace master {

struct ISchedulerVisitor
{
    virtual ~ISchedulerVisitor() {}
    virtual void Visit( Scheduler &scheduler ) = 0;
};

struct ICronVisitor
{
    virtual ~ICronVisitor() {}
    virtual void Visit( CronManager &cron ) = 0;
};

class InfoGetter
{
public:
    void GetInfo( std::string &info ) { info = info_; }

protected:
    std::string info_;
};

class JobInfo : public ISchedulerVisitor, public InfoGetter
{
public:
    JobInfo( int64_t jobId )
    : jobId_( jobId )
    {}

    virtual void Visit( Scheduler &scheduler );

    static void PrintJobInfo( std::string &info, Scheduler &scheduler, int64_t jobId );

private:
    int64_t jobId_;
};

class AllJobInfo : public ISchedulerVisitor, public InfoGetter
{
public:
    virtual void Visit( Scheduler &scheduler );
};

class Statistics : public ISchedulerVisitor, public InfoGetter
{
public:
    virtual void Visit( Scheduler &scheduler );

private:
    int GetNumBusyWorkers( Scheduler &scheduler ) const;
    int GetNumFreeWorkers( Scheduler &scheduler ) const;
    int GetNumBusyCPU( Scheduler &scheduler ) const;
};

class WorkerStatistics : public ISchedulerVisitor, public InfoGetter
{
public:
    virtual void Visit( Scheduler &scheduler );
};

class CronStatistics : public ICronVisitor, public InfoGetter
{
public:
    virtual void Visit( CronManager &cron );
};

} // namespace master

#endif

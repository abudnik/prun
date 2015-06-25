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

#ifndef __SCHEDULER_H
#define __SCHEDULER_H

#include <set>
#include <map>
#include <list>
#include <boost/bimap/bimap.hpp>
#include <boost/bimap/multiset_of.hpp>
#include <mutex>
#include "common/observer.h"
#include "worker.h"
#include "job.h"
#include "failed_workers.h"
#include "scheduled_jobs.h"
#include "node_state.h"
#include "worker_priority.h"


namespace master {

class ISchedulerVisitor;

struct IScheduler : virtual public common::IObservable
{
    virtual void OnHostAppearance( WorkerPtr &worker ) = 0;
    virtual void DeleteWorker( const std::string &host ) = 0;

    virtual void OnChangedWorkerState( std::vector< WorkerPtr > &workers ) = 0;

    virtual void OnNewJob() = 0;

    virtual bool GetTaskToSend( WorkerJob &workerJob, std::string &hostIP, JobPtr &job ) = 0;

    virtual void OnTaskSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP ) = 0;

    virtual void OnTaskCompletion( int errCode, int64_t execTime, const WorkerTask &workerTask, const std::string &hostIP ) = 0;

    virtual void OnTaskTimeout( const WorkerTask &workerTask, const std::string &hostIP ) = 0;
    virtual void OnJobTimeout( int64_t jobId ) = 0;

    virtual void StopJob( int64_t jobId ) = 0;
    virtual void StopJobGroup( int64_t groupId ) = 0;
    virtual void StopNamedJob( const std::string &name ) = 0;
    virtual void StopAllJobs() = 0;
    virtual void StopPreviousJobs() = 0;

    virtual void Accept( ISchedulerVisitor *visitor ) = 0;
};

using namespace boost::bimaps;

class Scheduler : public IScheduler,
                  public common::Observable< common::MutexLockPolicy >
{
private:
    typedef bimap< set_of< std::string >, multiset_of< NodeState *, CompareByCPUandMemory > > NodePriorityQueue; // ip -> NodeState

public:
    typedef std::map< std::string, NodeState > IPToNodeState;
    typedef std::list< WorkerTask > TaskList;
    typedef std::map< int64_t, std::set< int > > JobIdToTasks; // job_id -> set(task_id)
    typedef std::map< int64_t, int > JobIdToExecCnt; // job_id -> number of simultaneously running instances of the job

public:
    Scheduler();

    virtual void OnHostAppearance( WorkerPtr &worker );
    virtual void DeleteWorker( const std::string &host );

    virtual void OnChangedWorkerState( std::vector< WorkerPtr > &workers );

    virtual void OnNewJob();

    virtual bool GetTaskToSend( WorkerJob &workerJob, std::string &hostIP, JobPtr &job );

    virtual void OnTaskSendCompletion( bool success, const WorkerJob &workerJob, const std::string &hostIP );

    virtual void OnTaskCompletion( int errCode, int64_t execTime, const WorkerTask &workerTask, const std::string &hostIP );

    virtual void OnTaskTimeout( const WorkerTask &workerTask, const std::string &hostIP );
    virtual void OnJobTimeout( int64_t jobId );

    virtual void StopJob( int64_t jobId );
    virtual void StopJobGroup( int64_t groupId );
    virtual void StopNamedJob( const std::string &name );
    virtual void StopAllJobs();
    virtual void StopPreviousJobs();

    virtual void Accept( ISchedulerVisitor *visitor );

    const IPToNodeState &GetNodeState() const { return nodeState_; }
    const FailedWorkers &GetFailedWorkers() const { return failedWorkers_; }
    const TaskList &GetNeedReschedule() const { return needReschedule_; }
    ScheduledJobs &GetScheduledJobs() { return jobs_; }

private:
    void UpdateNodePriority( const std::string &ip, NodeState *nodeState );

    void PlanJobExecution();
    bool RescheduleJob( const WorkerJob &workerJob );

    bool GetReschedJobForWorker( const WorkerPtr &worker, WorkerJob &plannedJob, JobPtr &job, int numFreeCPU );
    bool GetJobForWorker( const WorkerPtr &worker, WorkerJob &plannedJob, JobPtr &job, int numFreeCPU );

    void OnRemoveJob( int64_t jobId, const std::string &jobName, bool success );
    void StopWorkers( int64_t jobId );
    void StopWorker( const std::string &hostIP ) const;

    bool CanTakeNewJob();
    bool CanAddTaskToWorker( const WorkerPtr &worker, const WorkerJob &workerPlannedJob,
                             int64_t jobId, const JobPtr &job ) const;

    int GetNumPlannedExec( const JobPtr &job ) const;

private:
    IPToNodeState nodeState_;
    NodePriorityQueue nodePriority_;
    FailedWorkers failedWorkers_;
    std::mutex workersMut_;

    ScheduledJobs jobs_;
    JobIdToTasks tasksToSend_;
    TaskList needReschedule_;
    JobIdToExecCnt simultExecCnt_;
    JobExecHistory history_;
    std::mutex jobsMut_;
};

} // namespace master

#endif

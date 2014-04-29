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

#include "statistics.h"
#include "worker_manager.h"
#include "common/service_locator.h"

namespace master {

void JobInfo::Visit( Scheduler &scheduler )
{
    PrintJobInfo( info_, scheduler, jobId_ );
}

void JobInfo::PrintJobInfo( std::string &info, Scheduler &scheduler, int64_t jobId )
{
    std::ostringstream ss;

    ScheduledJobs &jobs = scheduler.GetScheduledJobs();
    JobPtr job;
    if ( !jobs.FindJobByJobId( jobId, job ) )
     {
        ss << "job isn't executing now, jobId = " << jobId;
        info = ss.str();
        return;
    }

    ss << "================" << std::endl <<
        "Job info, jobId = " << job->GetJobId() << std::endl;

    if ( job->GetGroupId() >= 0 )
    {
        ss << "group id = " << job->GetGroupId() << std::endl;
    }

    if ( !job->GetAlias().empty() )
    {
        ss << "job alias = '" << job->GetAlias() << "'" << std::endl;
    }
    else
    {
        ss << "job path = '" << job->GetFilePath() << "'" << std::endl;
    }

    ss << "----------------" << std::endl;

    {
        int totalExec = job->GetNumPlannedExec();
        int numExec = totalExec - jobs.GetNumExec( jobId );
        ss << "job executions = " << numExec << std::endl <<
            "total planned executions = " << totalExec << std::endl;
    }

    {
        int numWorkers = 0;
        int numCPU = 0;
        const Scheduler::IPToNodeState &nodeState = scheduler.GetNodeState();
        Scheduler::IPToNodeState::const_iterator it = nodeState.begin();
        for( ; it != nodeState.end(); ++it )
        {
            const NodeState &nodeState = it->second;
            const WorkerPtr &worker = nodeState.GetWorker();
            if ( !worker )
                continue;

            const WorkerJob &workerJob = worker->GetJob();

            if ( workerJob.HasJob( jobId ) )
            {
                ++numWorkers;
                numCPU += workerJob.GetNumTasks( jobId );
            }
        }
        ss << "busy workers = " << numWorkers << std::endl;
        ss << "busy cpu's = " << numCPU << std::endl;
    }

    ss << "================";
    info = ss.str();
}

void AllJobInfo::Visit( Scheduler &scheduler )
{
    const ScheduledJobs &schedJobs = scheduler.GetScheduledJobs();
    const ScheduledJobs::JobQueue &jobs = schedJobs.GetJobQueue();

    ScheduledJobs::JobQueue::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        const JobPtr &job = *it;
        std::string jobInfo;
        JobInfo::PrintJobInfo( jobInfo, scheduler, job->GetJobId() );
        info_ += jobInfo + '\n';
    }
}

void Statistics::Visit( Scheduler &scheduler )
{
    std::ostringstream ss;

    const ScheduledJobs &schedJobs = scheduler.GetScheduledJobs();
    const FailedWorkers &failedWorkers = scheduler.GetFailedWorkers();

    IWorkerManager *workerManager = common::ServiceLocator::Instance().Get< IWorkerManager >();

    ss << "================" << std::endl <<
        "busy workers = " << GetNumBusyWorkers( scheduler ) << std::endl <<
        "free workers = " << GetNumFreeWorkers( scheduler ) << std::endl <<
        "failed jobs = " << failedWorkers.GetFailedJobsCnt() << std::endl <<
        "busy cpu's = " << GetNumBusyCPU( scheduler ) << std::endl <<
        "total cpu's = " << workerManager->GetTotalCPU() << std::endl;

    const Scheduler::TaskList &needReschedule = scheduler.GetNeedReschedule();

    ss << "jobs = " << schedJobs.GetNumJobs() << std::endl <<
        "need reschedule = " << needReschedule.size() << std::endl;

    ss << "executing jobs: {";
    const ScheduledJobs::JobQueue &jobs = schedJobs.GetJobQueue();
    ScheduledJobs::JobQueue::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        if ( it != jobs.begin() )
            ss << ", ";
        const JobPtr &job = *it;
        ss << job->GetJobId();
    }
    ss << "}" << std::endl;

    ss << "================";

    info_ = ss.str();
}

int Statistics::GetNumBusyWorkers( Scheduler &scheduler ) const
{
    const Scheduler::IPToNodeState &nodeState = scheduler.GetNodeState();

    int num = 0;
    Scheduler::IPToNodeState::const_iterator it = nodeState.begin();
    for( ; it != nodeState.end(); ++it )
    {
        const NodeState &nodeState = it->second;
        const WorkerPtr &worker = nodeState.GetWorker();
        if ( !worker || !worker->IsAvailable() )
            continue;

        if ( nodeState.GetNumBusyCPU() > 0 )
            ++num;
    }
    return num;
}

int Statistics::GetNumFreeWorkers( Scheduler &scheduler ) const
{
    const Scheduler::IPToNodeState &nodeState = scheduler.GetNodeState();

    int num = 0;
    Scheduler::IPToNodeState::const_iterator it = nodeState.begin();
    for( ; it != nodeState.end(); ++it )
    {
        const NodeState &nodeState = it->second;
        const WorkerPtr &worker = nodeState.GetWorker();
        if ( !worker || !worker->IsAvailable() )
            continue;

        if ( nodeState.GetNumBusyCPU() <= 0 )
            ++num;
    }
    return num;
}

int Statistics::GetNumBusyCPU( Scheduler &scheduler ) const
{
    const Scheduler::Scheduler::IPToNodeState &nodeState = scheduler.GetNodeState();

    int num = 0;
    Scheduler::IPToNodeState::const_iterator it = nodeState.begin();
    for( ; it != nodeState.end(); ++it )
    {
        const NodeState &nodeState = it->second;
        const WorkerPtr &worker = nodeState.GetWorker();
        if ( !worker || !worker->IsAvailable() )
            continue;

        num += nodeState.GetNumBusyCPU();
    }
    return num;
}

void WorkerStatistics::Visit( Scheduler &scheduler )
{
    std::ostringstream ss;

    const Scheduler::IPToNodeState &nodeState = scheduler.GetNodeState();

    ss << "================" << std::endl;

    Scheduler::IPToNodeState::const_iterator it, it_beg = nodeState.begin();
    for( it = it_beg ; it != nodeState.end(); ++it )
    {
        const NodeState &nodeState = it->second;
        const WorkerPtr &worker = nodeState.GetWorker();
        if ( !worker )
            continue;

        if ( it != it_beg )
            ss << "----------------" << std::endl;

        ss << "host = '" << worker->GetHost() << "', ip = " << worker->GetIP() << std::endl;

        if ( !worker->GetGroup().empty() )
        {
            ss << "group = " << worker->GetGroup() << std::endl;
        }

        const WorkerJob &workerJob = worker->GetJob();

        ss << "num cpu = " << worker->GetNumCPU() << std::endl <<
            "memory = " << worker->GetMemorySize() << std::endl <<
            "num executing tasks = " << workerJob.GetTotalNumTasks() << std::endl;

        ss << "tasks = {";
        std::vector< WorkerTask > tasks;
        workerJob.GetTasks( tasks );
        std::vector< WorkerTask >::const_iterator it = tasks.begin();
        for( ; it != tasks.end(); ++it )
        {
            const WorkerTask &task = *it;
            if ( it != tasks.begin() )
                ss << ",";
            ss << "(jobId=" << task.GetJobId() << ", taskId=" << task.GetTaskId() << ")";
        }
        ss << "}" << std::endl;
    }

    ss << "================";

    info_ = ss.str();
}

} // namespace master

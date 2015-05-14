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

#ifndef __JOB_H
#define __JOB_H

#include <list>
#include <vector>
#include <mutex>
#include <memory>
#include <boost/property_tree/ptree.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <stdint.h> // int64_t
#include "common/cron.h"

namespace master {

enum JobFlag
{
    JOB_FLAG_NO_RESCHEDULE = 1,
    JOB_FLAG_EXCLUSIVE = 2
};

typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::bidirectionalS > JobGraph;

typedef boost::graph_traits<JobGraph>::vertex_descriptor JobVertex;

class Job;
typedef std::weak_ptr< Job > JobWeakPtr;
typedef std::shared_ptr< Job > JobPtr;

class JobGroup;
typedef std::shared_ptr< JobGroup > JobGroupPtr;

struct IJobGroupEventReceiver
{
    virtual void OnJobDependenciesResolved( const JobPtr &job ) = 0;
};
typedef IJobGroupEventReceiver *IJobGroupEventReceiverPtr;

class JobGroup
{
public:
    typedef boost::property_map< JobGraph, boost::vertex_index_t >::type PropertyMap;

public:
    JobGroup( IJobGroupEventReceiverPtr &evReceiver );

    bool OnJobCompletion( const JobVertex &vertex );

    JobGraph &GetGraph() { return graph_; }

    std::vector< JobWeakPtr > &GetIndexToJob() { return indexToJob_; }

    const std::string &GetDescription() const { return description_; }
    void SetDescription( const std::string &description ) { description_ = description; }

    common::CronJob &GetCron() { return cron_; }

private:
    JobGraph graph_;
    std::vector< JobWeakPtr > indexToJob_;
    IJobGroupEventReceiverPtr eventReceiver_;
    size_t numCompleted_;
    std::string description_;
    common::CronJob cron_;
};

class Job
{
public:
    Job( const std::string &script, const std::string &scriptLanguage,
         int priority, int maxFailedNodes, int numExec,
         int maxClusterInstances, int maxWorkerInstances,
         int timeout, int queueTimeout, int taskTimeout,
         bool exclusive, bool noReschedule )
    : script_( script ), scriptLanguage_( scriptLanguage ),
     priority_( priority ), numDepends_( 0 ), maxFailedNodes_( maxFailedNodes ),
     numExec_( numExec ), maxClusterInstances_( maxClusterInstances ), maxWorkerInstances_( maxWorkerInstances ),
     timeout_( timeout ), queueTimeout_( queueTimeout ), taskTimeout_( taskTimeout ),
     flags_( 0 ), id_( -1 ), groupId_( -1 )
    {
        if ( noReschedule )
            flags_ |= JOB_FLAG_NO_RESCHEDULE;
        if ( exclusive )
            flags_ |= JOB_FLAG_EXCLUSIVE;

        scriptLength_ = script_.size();
    }

    bool ReleaseJobGroup();

    const std::string &GetScript() const { return script_; }
    const std::string &GetScriptLanguage() const { return scriptLanguage_; }
    unsigned int GetScriptLength() const { return scriptLength_; }

    const std::string &GetFilePath() const { return filePath_; }
    const std::string &GetAlias() const { return alias_; }
    const std::string &GetDescription() const { return description_; }
    int GetPriority() const { return priority_; }
    int GetNumDepends() const { return numDepends_; }
    int GetNumPlannedExec() const { return numPlannedExec_; }
    int GetMaxFailedNodes() const { return maxFailedNodes_; }
    int GetNumExec() const { return numExec_; }
    int GetMaxClusterInstances() const { return maxClusterInstances_; }
    int GetMaxWorkerInstances() const { return maxWorkerInstances_; }
    int GetTimeout() const { return timeout_; }
    int GetQueueTimeout() const { return queueTimeout_; }
    int GetTaskTimeout() const { return taskTimeout_; }
    bool IsNoReschedule() const { return flags_ & JOB_FLAG_NO_RESCHEDULE; }
    bool IsExclusive() const { return flags_ & JOB_FLAG_EXCLUSIVE; }
    int64_t GetJobId() const { return id_; }
    int64_t GetGroupId() const { return groupId_; }
    common::CronJob &GetCron() { return cron_; }
    JobGroupPtr GetJobGroup() { return jobGroup_; }

    void SetFilePath( const std::string &filePath ) { filePath_ = filePath; }
    void SetAlias( const std::string &alias ) { alias_ = alias; }
    void SetDescription( const std::string &description ) { description_ = description; }
    void SetNumPlannedExec( int val ) { numPlannedExec_ = val; }
    void SetNumDepends( int val ) { numDepends_ = val; }
    void SetJobId( int64_t val ) { id_ = val; }
    void SetGroupId( int64_t val ) { groupId_ = val; }
    void SetJobVertex( const JobVertex &vertex ) { graphVertex_ = vertex; }
    void SetJobGroup( const JobGroupPtr &jobGroup ) { jobGroup_ = jobGroup; }

    void AddHost( const std::string &host ) { hosts_.insert( host ); }
    bool IsHostPermitted( const std::string &host ) const;

    void AddGroup( const std::string &group ) { groups_.insert( group ); }
    bool IsGroupPermitted( const std::string &group ) const;

    template< typename T, typename U >
    void SetCallback( T *obj, void (U::*f)( const std::string &method, const boost::property_tree::ptree &params ) )
    {
        callback_ = std::bind( f, obj->shared_from_this(), std::placeholders::_1, std::placeholders::_2 );
    }

    void RunCallback( const std::string &method, const boost::property_tree::ptree &params ) const
    {
        if ( callback_ )
            callback_( method, params );
    }

private:
    std::string script_;
    std::string scriptLanguage_;
    unsigned int scriptLength_;
    std::string filePath_;
    std::string alias_;
    std::string description_;

    int priority_;
    int numDepends_;
    int numPlannedExec_;
    int maxFailedNodes_;
    int numExec_;
    int maxClusterInstances_;
    int maxWorkerInstances_;
    int timeout_, queueTimeout_, taskTimeout_;
    int flags_;
    int64_t id_;
    int64_t groupId_;

    std::set< std::string > hosts_;
    std::set< std::string > groups_;
    common::CronJob cron_;

    JobVertex graphVertex_;
    JobGroupPtr jobGroup_;
    std::function< void (const std::string &method, const boost::property_tree::ptree &params) > callback_;
};

struct JobComparatorPriority
{
    bool operator() ( const JobPtr &a, const JobPtr &b ) const
    {
        if ( a->GetPriority() > b->GetPriority() )
            return true;
        if ( a->GetPriority() == b->GetPriority() )
        {
            if ( a->GetGroupId() > b->GetGroupId() )
                return true;
        }
        return false;
    }
};


class IJobQueue
{
public:
    virtual ~IJobQueue() {}

    virtual void PushJob( JobPtr &job, int64_t groupId ) = 0;
    virtual void PushJobs( std::list< JobPtr > &jobs, int64_t groupId ) = 0;

    virtual bool PopJob( JobPtr &job ) = 0;

    virtual void OnJobDependenciesResolved( const JobPtr &job ) = 0;

    virtual bool GetJobById( int64_t jobId, JobPtr &job ) = 0;

    virtual bool DeleteJob( int64_t jobId ) = 0;
    virtual bool DeleteJobGroup( int64_t groupId ) = 0;
    virtual void Clear() = 0;
};

class JobQueue : public IJobQueue
{
    typedef std::map< int64_t, JobPtr > IdToJob;
    typedef std::vector< JobPtr > JobList;
    typedef std::set< JobPtr > JobSet;

public:
    virtual void PushJob( JobPtr &job, int64_t groupId );
    virtual void PushJobs( std::list< JobPtr > &jobs, int64_t groupId );

    virtual bool PopJob( JobPtr &job );

    virtual void OnJobDependenciesResolved( const JobPtr &job );

    virtual bool GetJobById( int64_t jobId, JobPtr &job );

    virtual bool DeleteJob( int64_t jobId );
    virtual bool DeleteJobGroup( int64_t groupId );
    virtual void Clear();

private:
    void OnJobDeletion( JobPtr &job ) const;

private:
    JobList jobs_;
    JobSet delayedJobs_; // jobs with unresolved dependencies
    IdToJob idToJob_;
    std::recursive_mutex jobsMut_;
};

} // namespace master

#endif

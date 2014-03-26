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

#ifndef __WORKER_MANAGER_H
#define __WORKER_MANAGER_H

#include <list>
#include <queue>
#include <utility> // pair
#include <boost/thread/mutex.hpp>
#include "common/observer.h"
#include "worker.h"
#include "command.h"

namespace master {

class IWorkerManager
{
protected:
    typedef std::map< std::string, WorkerList > GrpNameToWorkerList;

public:
    virtual void AddWorkerGroup( const std::string &groupName, std::list< std::string > &hosts ) = 0;
    virtual void AddWorkerHost( const std::string &groupName, const std::string &host ) = 0;

    virtual void DeleteWorkerGroup( const std::string &groupName ) = 0;
    virtual void DeleteWorkerHost( const std::string &host ) = 0;

    virtual void CheckDropedPingResponses() = 0;

    virtual void OnNodePingResponse( const std::string &hostIP, int numCPU, int64_t memSizeMb ) = 0;

    virtual void OnNodeTaskCompletion( const std::string &hostIP, int64_t jobId, int taskId ) = 0;

    virtual bool GetAchievedTask( WorkerTask &worker, std::string &hostIP ) = 0;

    virtual void SetWorkerIP( WorkerPtr &worker, const std::string &ip ) = 0;
    virtual bool GetWorkerByIP( const std::string &ip, WorkerPtr &worker ) const = 0;

    virtual void AddCommand( CommandPtr &command, const std::string &hostIP ) = 0;
    virtual bool GetCommand( CommandPtr &command, std::string &hostIP ) = 0;

    virtual int GetTotalWorkers() const = 0;
    virtual int GetTotalCPU() const = 0;

    virtual const std::string &GetConfigDir() const = 0;

    template< typename Container >
    void GetWorkers( Container &workers ) const
    {
        boost::mutex::scoped_lock scoped_lock( GetWorkersMutex() );

        const GrpNameToWorkerList &workerGroups = GetWorkerGroups();
        GrpNameToWorkerList::const_iterator it = workerGroups.begin();
        for( ; it != workerGroups.end(); ++it )
        {
            const WorkerList &workerList = it->second;
            const WorkerList::WorkerContainer &w = workerList.GetWorkers();
            WorkerList::WorkerContainer::const_iterator w_it = w.begin();
            for( ; w_it != w.end(); ++w_it )
            {
                workers.push_back( *w_it );
            }
        }
    }

    template< typename Container >
    void GetWorkers( Container &workers, const std::string &groupName ) const
    {
        boost::mutex::scoped_lock scoped_lock( GetWorkersMutex() );

        const GrpNameToWorkerList &workerGroups = GetWorkerGroups();
        GrpNameToWorkerList::const_iterator it = workerGroups.find( groupName );
        if ( it != workerGroups.end() )
        {
            const WorkerList &workerList = it->second;
            const WorkerList::WorkerContainer &w = workerList.GetWorkers();
            WorkerList::WorkerContainer::const_iterator w_it = w.begin();
            for( ; w_it != w.end(); ++w_it )
            {
                workers.push_back( *w_it );
            }
        }
    }

protected:
    virtual const GrpNameToWorkerList &GetWorkerGroups() const = 0;
    virtual boost::mutex &GetWorkersMutex() const = 0;
};

class WorkerManager : public IWorkerManager,
                      public common::Observable< common::MutexLockPolicy >
{
public:
    enum ObserverEvent { eTaskCompletion, eCommand };

public:
    virtual void AddWorkerGroup( const std::string &groupName, std::list< std::string > &hosts );
    virtual void AddWorkerHost( const std::string &groupName, const std::string &host );

    virtual void DeleteWorkerGroup( const std::string &groupName );
    virtual void DeleteWorkerHost( const std::string &host );

    virtual void CheckDropedPingResponses();

    virtual void OnNodePingResponse( const std::string &hostIP, int numCPU, int64_t memSizeMb );

    virtual void OnNodeTaskCompletion( const std::string &hostIP, int64_t jobId, int taskId );

    virtual bool GetAchievedTask( WorkerTask &worker, std::string &hostIP );

    virtual void SetWorkerIP( WorkerPtr &worker, const std::string &ip );
    virtual bool GetWorkerByIP( const std::string &ip, WorkerPtr &worker ) const;

    virtual void AddCommand( CommandPtr &command, const std::string &hostIP );
    virtual bool GetCommand( CommandPtr &command, std::string &hostIP );

    virtual int GetTotalWorkers() const;
    virtual int GetTotalCPU() const;

    virtual const std::string &GetConfigDir() const { return cfgDir_; }

    static WorkerManager &Instance()
    {
        static WorkerManager instance_;
        return instance_;
    }

    void Initialize( const std::string &cfgDir );
    void Shutdown();

private:
    virtual const GrpNameToWorkerList &GetWorkerGroups() const { return workerGroups_; }
    virtual boost::mutex &GetWorkersMutex() const { return workersMut_; }

private:
    std::string cfgDir_;

    GrpNameToWorkerList workerGroups_;
    std::set< std::string > workerHosts_;
    mutable boost::mutex workersMut_;

    typedef std::pair< WorkerTask, std::string > PairTypeAW;
    std::queue< PairTypeAW > achievedWorkers_;
    boost::mutex achievedMut_;

    typedef std::pair< CommandPtr, std::string > PairTypeC;
    std::queue< PairTypeC > commands_;
    boost::mutex commandsMut_;
};

bool ReadHosts( const char *filePath, std::list< std::string > &hosts );

} // namespace master

#endif

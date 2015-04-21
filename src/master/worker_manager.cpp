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

#include <fstream>
#include <boost/asio/ip/address.hpp>
#include "worker_manager.h"
#include "common/log.h"
#include "common/service_locator.h"
#include "scheduler.h"

namespace master {

void WorkerManager::AddWorkerGroup( const std::string &groupName, std::list< std::string > &hosts )
{
    std::list< std::string >::const_iterator it = hosts.begin();
    for( ; it != hosts.end(); ++it )
    {
        AddWorkerHost( groupName, *it );
    }
}

void WorkerManager::AddWorkerHost( const std::string &groupName, const std::string &host )
{
    std::unique_lock< std::mutex > lock( workersMut_ );

    if ( workerHosts_.find( host ) != workerHosts_.end() )
    {
        PLOG( "WorkerManager::AddWorkerHost: host already exists '" << host << "'" );
        return;
    }
    workerHosts_.insert( host );

    WorkerList &workerList = workerGroups_[ groupName ];
    workerList.AddWorker( new Worker( host, groupName ) );
}

void WorkerManager::DeleteWorkerGroup( const std::string &groupName )
{
    std::unique_lock< std::mutex > lock( workersMut_ );

    GrpNameToWorkerList::iterator it = workerGroups_.find( groupName );
    if ( it != workerGroups_.end() )
    {
        WorkerList &workerList = it->second;
        const WorkerList::WorkerContainer &workers = workerList.GetWorkers();
        WorkerList::WorkerContainer::const_iterator it_w = workers.begin();
        for( ; it_w != workers.end(); ++it_w )
        {
            const std::string &host = (*it_w)->GetHost();
            workerHosts_.erase( host );
        }

        workerList.Clear();
        workerGroups_.erase( it );
    }
}

void WorkerManager::DeleteWorkerHost( const std::string &host )
{
    std::unique_lock< std::mutex > lock( workersMut_ );

    GrpNameToWorkerList::iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        WorkerList &workerList = it->second;
        workerList.DeleteWorker( host );
    }

    workerHosts_.erase( host );
}

void WorkerManager::CheckDropedPingResponses()
{
    std::vector< WorkerPtr > changedWorkers;

    std::unique_lock< std::mutex > lock( workersMut_ );

    GrpNameToWorkerList::const_iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        const WorkerList &workerList = it->second;

        const WorkerList::WorkerContainer &workers = workerList.GetWorkers();
        WorkerList::WorkerContainer::const_iterator it = workers.begin();
        for( ; it != workers.end(); ++it )
        {
            const WorkerPtr &worker = *it;
            WorkerState state = worker->GetState();
            if ( !worker->GetNumPingResponse() )
            {
                if ( state == WORKER_STATE_READY )
                {
                    worker->SetState( WORKER_STATE_NOT_AVAIL );
                    changedWorkers.push_back( worker );
                    PLOG( "WorkerManager::CheckDropedPingResponses: node not available, ip= " << worker->GetIP() );
                }
                if ( state == WORKER_STATE_EXEC )
                {
                    worker->SetState( WORKER_STATE_NOT_AVAIL );
                    changedWorkers.push_back( worker );
                    PLOG( "WorkerManager::CheckDropedPingResponses: node job isn't available, ip= " << worker->GetIP() );
                }
            }
            worker->SetNumPingResponse( 0 );
        }
    }

    lock.unlock();

    if ( !changedWorkers.empty() )
    {
        IScheduler *scheduler = common::ServiceLocator::Instance().Get< IScheduler >();
        scheduler->OnChangedWorkerState( changedWorkers );
    }
}

void WorkerManager::OnNodePingResponse( const std::string &hostIP, int numCPU, int64_t memSizeMb )
{
    WorkerPtr worker;
    if ( GetWorkerByIP( hostIP, worker ) )
    {
        bool stateChanged = false;
        worker->IncNumPingResponse();
        if ( worker->GetState() == WORKER_STATE_NOT_AVAIL )
        {
            worker->SetState( WORKER_STATE_READY );
            stateChanged = true;
            PLOG( "node available, ip= " << worker->GetIP() );
        }

        if ( stateChanged )
        {
            worker->SetNumCPU( numCPU );
            worker->SetMemorySize( memSizeMb );
            IScheduler *scheduler = common::ServiceLocator::Instance().Get< IScheduler >();
            scheduler->OnHostAppearance( worker );
        }
    }
    else
    {
        PLOG( "WorkerManager::OnHostPingResponse worker not found, ip= " << hostIP );
    }
}

void WorkerManager::OnNodeTaskCompletion( const std::string &hostIP, int64_t jobId, int taskId )
{
    if ( jobId < 0 || taskId < 0 )
        return;

    PairTypeAW worker( WorkerTask( jobId, taskId ), hostIP );

    {
        std::unique_lock< std::mutex > lock( achievedMut_ );
        achievedWorkers_.push( worker );
    }
    NotifyAll( eTaskCompletion );
}

bool WorkerManager::GetAchievedTask( WorkerTask &worker, std::string &hostIP )
{
    if ( achievedWorkers_.empty() )
        return false;

    std::unique_lock< std::mutex > lock( achievedMut_ );
    if ( achievedWorkers_.empty() )
        return false;

    PLOG( "GetAchievedWorker: num achieved workers=" << achievedWorkers_.size() );

    const PairTypeAW &w = achievedWorkers_.front();
    worker = w.first;
    hostIP = w.second;
    achievedWorkers_.pop();
    return true;
}

void WorkerManager::AddCommand( CommandPtr &command, const std::string &hostIP )
{
    PairTypeC workerCommand( command, hostIP );

    {
        std::unique_lock< std::mutex > lock( commandsMut_ );
        commands_.push( workerCommand );
    }
    NotifyAll( eCommand );
}

bool WorkerManager::GetCommand( CommandPtr &command, std::string &hostIP )
{
    if ( commands_.empty() )
        return false;

    std::unique_lock< std::mutex > lock( commandsMut_ );
    if ( commands_.empty() )
        return false;

    const PairTypeC &c = commands_.front();
    command = c.first;
    hostIP = c.second;
    commands_.pop();
    return true;
}

void WorkerManager::SetWorkerIP( WorkerPtr &worker, const std::string &ip )
{
    std::unique_lock< std::mutex > lock( workersMut_ );

    GrpNameToWorkerList::iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        WorkerList &workerList = it->second;
        workerList.SetWorkerIP( worker, ip );
    }
}

bool WorkerManager::GetWorkerByIP( const std::string &ip, WorkerPtr &worker ) const
{
    std::unique_lock< std::mutex > lock( workersMut_ );

    GrpNameToWorkerList::const_iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        const WorkerList &workerList = it->second;
        if ( workerList.GetWorkerByIP( ip, worker ) )
            return true;

    }
    return false;
}

int WorkerManager::GetTotalWorkers() const
{
    int total = 0;

    std::unique_lock< std::mutex > lock( workersMut_ );

    GrpNameToWorkerList::const_iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        const WorkerList &workerList = it->second;
        total += workerList.GetTotalWorkers();
    }
    return total;
}

int WorkerManager::GetTotalCPU() const
{
    int total = 0;

    std::unique_lock< std::mutex > lock( workersMut_ );

    GrpNameToWorkerList::const_iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        const WorkerList &workerList = it->second;
        total += workerList.GetTotalCPU();
    }
    return total;
}

void WorkerManager::Initialize( const std::string &cfgDir )
{
    cfgDir_ = cfgDir;
}

void WorkerManager::Shutdown()
{
    std::unique_lock< std::mutex > lock( workersMut_ );

    GrpNameToWorkerList::iterator it = workerGroups_.begin();
    for( ; it != workerGroups_.end(); ++it )
    {
        WorkerList &workerList = it->second;
        workerList.Clear();
    }
}

bool ReadHosts( const char *filePath, std::list< std::string > &hosts )
{
    int numHosts = 0;
    std::ifstream file( filePath );
    if ( !file.is_open() )
    {
        PLOG_ERR( "ReadHosts: couldn't open " << filePath );
        return false;
    }
    try
    {
        while( file.good() )
        {
            std::string host;
            getline( file, host );
            if ( host.empty() )
                continue;

            if ( isdigit( host[0] ) )
            {
                boost::system::error_code error;
                boost::asio::ip::address::from_string( host.c_str(), error );
                if ( error )
                {
                    PLOG_WRN( "invalid host ip: " << host );
                    continue;
                }
            }
            hosts.push_back( host );
            ++numHosts;
        }
    }
    catch( std::exception &e )
    {
        PLOG_ERR( "ReadHosts: failed " << e.what() );
        return false;
    }
    PLOG( numHosts << " hosts are readed" );
    return true;
}

} // namespace master

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

#ifndef __COMM_DESCRIPTOR_H
#define __COMM_DESCRIPTOR_H

#include <thread>
#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include "common/log.h"
#include "common/helper.h"
#include "common/config.h"
#include "common.h"

using boost::asio::local::stream_protocol;

namespace worker {

struct ThreadComm
{
    int connectId;
};

struct CommDescr
{
    CommDescr() {}

    CommDescr( const CommDescr &descr )
    {
        *this = descr;
    }

    CommDescr & operator = ( const CommDescr &descr )
    {
        if ( this != &descr )
        {
            shmemBlockId = descr.shmemBlockId;
            shmemAddr = descr.shmemAddr;
            socket = descr.socket;
            used = descr.used;
        }
        return *this;
    }

    unsigned int shmemBlockId;
    char *shmemAddr;
    std::shared_ptr< stream_protocol::socket > socket;
    bool used;
};

class CommDescrPool
{
    typedef std::map< std::thread::id, ThreadComm > CommParams;

public:
    CommDescrPool( unsigned int numJobThreads, boost::asio::io_service *io_service, char *shmemAddr )
    : sem_( new common::Semaphore( numJobThreads ) ),
     io_service_( io_service )
    {
        for( unsigned int i = 0; i < numJobThreads; ++i )
        {
            // init shmem block associated with created thread
            CommDescr commDescr;
            commDescr.shmemBlockId = i;
            commDescr.shmemAddr = shmemAddr + i * SHMEM_BLOCK_SIZE;
            commDescr.used = false;

            memset( commDescr.shmemAddr, 0, SHMEM_BLOCK_SIZE );

            boost::system::error_code ec;

            // open socket to pyexec
            commDescr.socket = std::make_shared< stream_protocol::socket >( *io_service );
            commDescr.socket->connect( stream_protocol::endpoint( UDS_NAME ) );
            if ( ec )
            {
                PLOG_ERR( "CommDescrPool(): socket_.connect() failed " << ec.message() );
                exit( 1 );
            }

            AddCommDescr( commDescr );
        }
    }

    CommDescr &GetCommDescr()
    {
        std::unique_lock< std::mutex > lock( commDescrMut_ );
        ThreadComm &threadComm = commParams_[ std::this_thread::get_id() ];
        return commDescr_[ threadComm.connectId ];
    }

    bool AllocCommDescr()
    {
        sem_->Wait();

        std::unique_lock< std::mutex > lock( commDescrMut_ );

        for( size_t i = 0; i < commDescr_.size(); ++i )
        {
            if ( !commDescr_[i].used )
            {
                commDescr_[i].used = true;
                ThreadComm &threadComm = commParams_[ std::this_thread::get_id() ];
                threadComm.connectId = i;
                return true;
            }
        }
        PLOG_WRN( "AllocCommDescr: available communication descriptor not found" );
        return false;
    }

    void FreeCommDescr()
    {
        {
            std::unique_lock< std::mutex > lock( commDescrMut_ );
            ThreadComm &threadComm = commParams_[ std::this_thread::get_id() ];
            commDescr_[ threadComm.connectId ].used = false;
            threadComm.connectId = -1;
        }
        sem_->Notify();
    }

    void Shutdown()
    {
        {
            std::unique_lock< std::mutex > lock( commDescrMut_ );
            std::vector< CommDescr >::iterator it = commDescr_.begin();
            for( ; it != commDescr_.end(); ++it )
            {
                CommDescr &descr = *it;
                boost::system::error_code error;
                descr.socket->shutdown( stream_protocol::socket::shutdown_both, error );
                descr.socket->close( error );
            }
            commDescr_.clear();
        }
        sem_->Reset();
    }

    boost::asio::io_service *GetIoService() const { return io_service_; }

private:
    void AddCommDescr( const CommDescr &descr )
    {
        std::unique_lock< std::mutex > lock( commDescrMut_ );
        commDescr_.push_back( descr );
    }

private:
    std::vector< CommDescr > commDescr_;
    std::mutex commDescrMut_;

    CommParams commParams_;
    boost::scoped_ptr< common::Semaphore > sem_;
    boost::asio::io_service *io_service_;
};

} // namespace worker

#endif

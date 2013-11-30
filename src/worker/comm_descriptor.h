#ifndef __COMM_DESCRIPTOR_H
#define __COMM_DESCRIPTOR_H

#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include "common/log.h"
#include "common/helper.h"
#include "common/config.h"
#include "common.h"

using boost::asio::ip::tcp;

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

    int shmemBlockId;
    char *shmemAddr;
    boost::shared_ptr< tcp::socket > socket;
    bool used;
};

class CommDescrPool
{
    typedef std::map< boost::thread::id, ThreadComm > CommParams;

public:
    CommDescrPool( int numJobThreads, boost::asio::io_service *io_service, char *shmemAddr )
    : sem_( new common::Semaphore( numJobThreads ) ),
     io_service_( io_service ), bWorking_( true )
    {
        for( int i = 0; i < numJobThreads; ++i )
        {
            // init shmem block associated with created thread
            CommDescr commDescr;
            commDescr.shmemBlockId = i;
            commDescr.shmemAddr = shmemAddr + i * SHMEM_BLOCK_SIZE;
            commDescr.used = false;

            memset( commDescr.shmemAddr, 0, SHMEM_BLOCK_SIZE );

            boost::system::error_code ec;

            // open socket to pyexec
            tcp::resolver resolver( *io_service );

            common::Config &cfg = common::Config::Instance();
            bool ipv6 = cfg.Get<bool>( "ipv6" );

            tcp::resolver::query query( ipv6 ? tcp::v6() : tcp::v4(), "localhost", boost::lexical_cast<std::string>( DEFAULT_PREXEC_PORT ) );
            tcp::resolver::iterator iterator = resolver.resolve( query, ec ), end;
            if ( ec || iterator == end )
            {
                PS_LOG( "CommDescrPool() address not resolved: 'localhost'" );
                exit( 1 );
            }

            commDescr.socket = boost::shared_ptr< tcp::socket >( new tcp::socket( *io_service ) );
            commDescr.socket->connect( *iterator, ec );
            if ( ec )
            {
                PS_LOG( "CommDescrPool(): socket_.connect() failed " << ec.message() );
                exit( 1 );
            }

            AddCommDescr( commDescr );
        }
    }

    CommDescr &GetCommDescr()
    {
        boost::unique_lock< boost::mutex > lock( commDescrMut_ );
        ThreadComm &threadComm = commParams_[ boost::this_thread::get_id() ];
        return commDescr_[ threadComm.connectId ];
    }

    bool AllocCommDescr()
    {
        while( 1 )
        {
            sem_->Wait();
            {
                boost::unique_lock< boost::mutex > lock( commDescrMut_ );

                if ( !bWorking_ )
                    break;

                for( size_t i = 0; i < commDescr_.size(); ++i )
                {
                    if ( !commDescr_[i].used )
                    {
                        commDescr_[i].used = true;
                        ThreadComm &threadComm = commParams_[ boost::this_thread::get_id() ];
                        threadComm.connectId = i;
                        return true;
                    }
                }
            }
            PS_LOG( "AllocCommDescr: available communication descriptor not found" );
        }
        return false;
    }

    void FreeCommDescr()
    {
        {
            boost::unique_lock< boost::mutex > lock( commDescrMut_ );
            ThreadComm &threadComm = commParams_[ boost::this_thread::get_id() ];
            commDescr_[ threadComm.connectId ].used = false;
            threadComm.connectId = -1;
        }
        sem_->Notify();
    }

    void Shutdown()
    {
        {
            boost::unique_lock< boost::mutex > lock( commDescrMut_ );
            bWorking_ = false;
        }
        sem_->Reset();
    }

    boost::asio::io_service *GetIoService() const { return io_service_; }

private:
    void AddCommDescr( const CommDescr &descr )
    {
        boost::unique_lock< boost::mutex > lock( commDescrMut_ );
        commDescr_.push_back( descr );
    }

private:
    std::vector< CommDescr > commDescr_;
    boost::mutex commDescrMut_;

    CommParams commParams_;
    boost::scoped_ptr< common::Semaphore > sem_;
    boost::asio::io_service *io_service_;

    bool bWorking_;
};

} // namespace worker

#endif

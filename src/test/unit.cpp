#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Unit tests
#include <boost/test/unit_test.hpp>
#include <vector>
#include "master/worker_manager.h"
#include "master/scheduler.h"
#include "common/service_locator.h"

using namespace std;
using namespace master;


////////////////////////////////
// WorkerManager
////////////////////////////////

BOOST_FIXTURE_TEST_SUITE( WorkerManagerSuite, WorkerManager )

BOOST_AUTO_TEST_CASE( host_uniqueness )
{
    for( int i = 0; i < 5; ++i )
    {
        AddWorkerHost( "grp1", "host1" );
        AddWorkerHost( "grp2", "host2" );
        AddWorkerHost( "grp3", "host3" );
    }

    vector< WorkerPtr > workersVector;
    GetWorkers( workersVector, "grp2" );
    BOOST_CHECK_EQUAL( workersVector.size(), 1 );
}

BOOST_AUTO_TEST_CASE( group_removal )
{
    for( int i = 0; i < 5; ++i )
    {
        AddWorkerHost( "grp1", "host1" );
        AddWorkerHost( "grp2", "host2" );
        AddWorkerHost( "grp3", "host3" );
    }

    DeleteWorkerGroup( "grp2" );
    vector< WorkerPtr > workersVector;
    GetWorkers( workersVector, "grp2" );
    BOOST_CHECK( workersVector.empty() );
}

BOOST_AUTO_TEST_CASE( host_removal )
{
    for( int i = 0; i < 5; ++i )
    {
        AddWorkerHost( "grp2", "host1" );
        AddWorkerHost( "grp2", "host2" );
        AddWorkerHost( "grp2", "host3" );
    }

    DeleteWorkerHost( "host2" );
    vector< WorkerPtr > workersVector;
    GetWorkers( workersVector, "grp2" );
    BOOST_CHECK_EQUAL( workersVector.size(), 2 );
}

BOOST_AUTO_TEST_SUITE_END()


struct WorkerManagerEnvironment
{
    WorkerManagerEnvironment()
    {
        common::ServiceLocator &serviceLocator = common::ServiceLocator::Instance();
        serviceLocator.Register( (master::IScheduler*)&sched );
    }

    ~WorkerManagerEnvironment()
    {
        common::ServiceLocator::Instance().UnregisterAll();
    }

    WorkerManager mgr;
    Scheduler sched;
};

BOOST_FIXTURE_TEST_SUITE( WorkerManagerIntegrationSuite, WorkerManagerEnvironment )

BOOST_AUTO_TEST_CASE( host_ping_response )
{
    mgr.AddWorkerHost( "grp", "host1" );

    std::vector< WorkerPtr > workers;
    mgr.GetWorkers( workers );

    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    BOOST_CHECK_EQUAL( mgr.GetTotalWorkers(), 0 );
    mgr.SetWorkerIP( workers[0], "127.0.0.1" );
    mgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );
    BOOST_CHECK_EQUAL( mgr.GetTotalWorkers(), 1 );
}

BOOST_AUTO_TEST_SUITE_END()

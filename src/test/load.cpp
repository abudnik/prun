#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Load tests
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>
#include <list>
#include "mock.h"
#include "master/worker_manager.h"
#include "master/timeout_manager.h"
#include "master/job_manager.h"
#include "master/scheduler.h"
#include "common/service_locator.h"

using namespace std;
using namespace master;

////////////////////////////////////////////////////////////////
// Scheduler
////////////////////////////////////////////////////////////////

struct SchedulerEnvironment
{
    SchedulerEnvironment()
    {
        jobMgr.SetTimeoutManager( &timeoutMgr );
        common::ServiceLocator &serviceLocator = common::ServiceLocator::Instance();
        serviceLocator.Register( (master::IScheduler*)&sched );
        serviceLocator.Register( (master::IJobManager*)&jobMgr );
        serviceLocator.Register( (master::IWorkerManager*)&workerMgr );
    }

    ~SchedulerEnvironment()
    {
        common::ServiceLocator::Instance().UnregisterAll();
    }

    MockTimeoutManager timeoutMgr;
    JobManager jobMgr;
    WorkerManager workerMgr;
    Scheduler sched;
};

BOOST_FIXTURE_TEST_SUITE( SchedulerSuite, SchedulerEnvironment )

BOOST_AUTO_TEST_CASE( get_task )
{
    const int numHosts = 10000;

    for( int i = 0; i < numHosts; ++i )
    {
        workerMgr.AddWorkerHost( "grp", string( "host" ) + boost::lexical_cast<string>( i + 1 ) );
    }

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), numHosts );

    for( int i = 0; i < numHosts; ++i )
    {
        string ip( "127.0.0." );
        ip += boost::lexical_cast<string>( i + 1 );
        workerMgr.SetWorkerIP( workers[i], ip );

        int numCPU = i % 4 + 1;
        workerMgr.OnNodePingResponse( ip, numCPU, 1024 );
    }

    const int numJobs = numHosts * 10;

    for( int i = 0; i < numJobs; ++i )
    {
        int priority = i % 10;
        Job *job = new Job( "", "python", priority, 10, 1, -1, 1,
                            1, 1, 1, false, false );
        BOOST_REQUIRE( job );
        jobMgr.PushJob( job );
    }

    int numTasksScheduled = 0;
    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;
    for( int i = 0; i < numJobs; ++i, ++numTasksScheduled )
    {
        workerJob.Reset();
        spJob.reset();

        if ( !sched.GetTaskToSend( workerJob, hostIP, spJob ) )
            break;
    }

    BOOST_TEST_MESSAGE( "NUM HOSTS: " << numHosts );
    BOOST_TEST_MESSAGE( "NUM JOBS: " << numJobs );
    BOOST_TEST_MESSAGE( "TASKS SCHEDULED: " << numTasksScheduled );

    /*
    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_REQUIRE_EQUAL( tasks.empty(), false );

    sched.OnTaskCompletion( -1, 10, tasks[0], hostIP ); // task failed

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP2, spJob ) );
    BOOST_CHECK( (bool)spJob );
    BOOST_CHECK_NE( hostIP, hostIP2 );

    sched.OnTaskCompletion( -1, 10, tasks[0], hostIP2 ); // task failed once again

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK_EQUAL( sched.GetTaskToSend( workerJob, hostIP2, spJob ), false ); // no more avail workers*/
}

BOOST_AUTO_TEST_SUITE_END()

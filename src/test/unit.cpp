#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Unit tests
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
// WorkerManager
////////////////////////////////////////////////////////////////

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

BOOST_AUTO_TEST_CASE( set_host_ip )
{
    AddWorkerHost( "grp", "host1" );
    AddWorkerHost( "grp", "host2" );
    AddWorkerHost( "grp", "host3" );

    vector< WorkerPtr > workers;
    WorkerPtr w;
    GetWorkers( workers );

    for( size_t i = 0; i < workers.size(); ++i )
    {
        string ip = "127.0.0." + boost::lexical_cast<std::string>( i + 1 );
        SetWorkerIP( workers[i], ip );

        w.reset();
        GetWorkerByIP( ip, w );
        BOOST_CHECK( (bool)w );
    }

    w.reset();
    GetWorkerByIP( "321", w );
    BOOST_CHECK_EQUAL( (bool)w, false );
}

BOOST_AUTO_TEST_CASE( check_command )
{
    CommandPtr cmd;
    string ip;
    BOOST_CHECK_EQUAL( GetCommand( cmd, ip ), false );

    CommandPtr newCmd( new MockCommand );
    AddCommand( newCmd, "127.0.0.1" );
    BOOST_CHECK( GetCommand( cmd, ip ) );
    BOOST_CHECK( (bool)cmd );
}

BOOST_AUTO_TEST_CASE( check_task_completion_queue )
{
    WorkerTask task;
    string ip;
    BOOST_CHECK_EQUAL( GetAchievedTask( task, ip ), false );

    const int numTasks = 5;

    for( int i = 0; i < numTasks; ++i )
    {
        OnNodeTaskCompletion( "127.0.0.1", i, i * 2 );
    }

    for( int i = 0; i < numTasks; ++i )
    {
        BOOST_CHECK( GetAchievedTask( task, ip ) );
    }

    BOOST_CHECK_EQUAL( GetAchievedTask( task, ip ), false );
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

BOOST_AUTO_TEST_CASE( node_ping_response )
{
    mgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    mgr.GetWorkers( workers );

    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    BOOST_CHECK_EQUAL( mgr.GetTotalWorkers(), 0 );
    mgr.SetWorkerIP( workers[0], "127.0.0.1" );
    mgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );
    BOOST_CHECK_EQUAL( mgr.GetTotalWorkers(), 1 );
}

BOOST_AUTO_TEST_CASE( check_ping_response )
{
    mgr.AddWorkerHost( "grp", "host1" );
    mgr.AddWorkerHost( "grp", "host2" );
    mgr.AddWorkerHost( "grp", "host3" );

    vector< WorkerPtr > workers;
    mgr.GetWorkers( workers );

    for( int k = 0; k < 3; ++k )
    {
        for( size_t i = 0; i < workers.size(); ++i )
        {
            string ip = "127.0.0." + boost::lexical_cast<std::string>( i + 1 );
            mgr.SetWorkerIP( workers[i], ip );
            mgr.OnNodePingResponse( ip, 2, 1024 );
        }
        BOOST_CHECK_EQUAL( mgr.GetTotalWorkers(), 3 );

        mgr.CheckDropedPingResponses(); // clear ping responses counter
        mgr.CheckDropedPingResponses();
        BOOST_CHECK_EQUAL( mgr.GetTotalWorkers(), 0 );
    }
}

BOOST_AUTO_TEST_CASE( check_total_cpu )
{
    const size_t numWorkers = 5;

    BOOST_CHECK_EQUAL( mgr.GetTotalCPU(), 0 );
    for( size_t i = 0; i < numWorkers; ++i )
    {
        mgr.AddWorkerHost( "grp", string( "host" ) + boost::lexical_cast<std::string>( i + 1 ) );
    }
    BOOST_CHECK_EQUAL( mgr.GetTotalCPU(), 0 );

    vector< WorkerPtr > workers;
    mgr.GetWorkers( workers );

    int numCPU = 0;
    for( size_t i = 0; i < workers.size(); ++i )
    {
        string ip = "127.0.0." + boost::lexical_cast<std::string>( i + 1 );
        mgr.SetWorkerIP( workers[i], ip );
        mgr.OnNodePingResponse( ip, i + 1, 1024 );
        numCPU += i + 1;
    }
    BOOST_CHECK_GT( mgr.GetTotalCPU(), 0 );
    BOOST_CHECK_EQUAL( mgr.GetTotalCPU(), numCPU );
}

BOOST_AUTO_TEST_SUITE_END()

////////////////////////////////////////////////////////////////
// JobManager
////////////////////////////////////////////////////////////////

struct JobManagerEnvironment
{
    JobManagerEnvironment()
    {
        mgr.SetTimeoutManager( &timeoutManager );
        common::ServiceLocator &serviceLocator = common::ServiceLocator::Instance();
        serviceLocator.Register( (master::IScheduler*)&sched );
    }

    ~JobManagerEnvironment()
    {
        common::ServiceLocator::Instance().UnregisterAll();
    }

    MockTimeoutManager timeoutManager;
    JobManager mgr;
    Scheduler sched;
};

BOOST_FIXTURE_TEST_SUITE( JobManagerSuite, JobManagerEnvironment )

BOOST_AUTO_TEST_CASE( job_creation )
{
    JobPtr job1( mgr.CreateJob(
                     "{\"script\" : \"simple.py\","
                     "\"language\" : \"python\","
                     "\"send_script\" : false,"
                     "\"priority\" : 4,"
                     "\"job_timeout\" : 120,"
                     "\"queue_timeout\" : 60,"
                     "\"task_timeout\" : 15,"
                     "\"max_failed_nodes\" : 10,"
                     "\"num_execution\" : 1,"
                     "\"max_cluster_cpu\" : -1,"
                     "\"max_cpu\" : 1,"
                     "\"exclusive\" : false,"
                     "\"no_reschedule\" : false}" ) );
    BOOST_CHECK( (bool)job1 );

    JobPtr job2( mgr.CreateJob( "_garbage_" ) );
    BOOST_CHECK_EQUAL( (bool)job2, false );

    JobPtr job3( mgr.CreateJob( "{\"_random_field_\" : 1}" ) );
    BOOST_CHECK_EQUAL( (bool)job3, false );
}

BOOST_AUTO_TEST_CASE( job_queue )
{
    const int numJobs = 10;

    for( int i = 0; i < numJobs; ++i )
    {
        Job *job( mgr.CreateJob(
                      "{\"script\" : \"simple.py\","
                      "\"language\" : \"python\","
                      "\"send_script\" : false,"
                      "\"priority\" : 4,"
                      "\"job_timeout\" : 120,"
                      "\"queue_timeout\" : 60,"
                      "\"task_timeout\" : 15,"
                      "\"max_failed_nodes\" : 10,"
                      "\"num_execution\" : 1,"
                      "\"max_cluster_cpu\" : -1,"
                      "\"max_cpu\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}" ) );
        BOOST_REQUIRE( job );
        mgr.PushJob( job );
    }

    for( int i = 0; i < numJobs; ++i )
    {
        JobPtr j;
        BOOST_CHECK( mgr.PopJob( j ) );
        BOOST_CHECK( (bool)j );
    }

    JobPtr j;
    BOOST_CHECK_EQUAL( mgr.PopJob( j ), false );
}

BOOST_AUTO_TEST_CASE( job_queue2 )
{
    const int numJobs = 10;

    list< JobPtr > jobs;
    for( int i = 0; i < numJobs; ++i )
    {
        Job *job( mgr.CreateJob(
                      "{\"script\" : \"simple.py\","
                      "\"language\" : \"python\","
                      "\"send_script\" : false,"
                      "\"priority\" : 4,"
                      "\"job_timeout\" : 120,"
                      "\"queue_timeout\" : 60,"
                      "\"task_timeout\" : 15,"
                      "\"max_failed_nodes\" : 10,"
                      "\"num_execution\" : 1,"
                      "\"max_cluster_cpu\" : -1,"
                      "\"max_cpu\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}" ) );
        BOOST_REQUIRE( job );
        jobs.push_back( JobPtr( job ) );
    }
    mgr.PushJobs( jobs );

    for( int i = 0; i < numJobs; ++i )
    {
        JobPtr j;
        BOOST_CHECK( mgr.PopJob( j ) );
        BOOST_CHECK( (bool)j );
    }

    JobPtr j;
    BOOST_CHECK_EQUAL( mgr.PopJob( j ), false );
}

BOOST_AUTO_TEST_CASE( job_get_by_id )
{
    const int numJobs = 10;

    list< JobPtr > jobs;
    for( int i = 0; i < numJobs; ++i )
    {
        Job *job( mgr.CreateJob(
                      "{\"script\" : \"simple.py\","
                      "\"language\" : \"python\","
                      "\"send_script\" : false,"
                      "\"priority\" : 4,"
                      "\"job_timeout\" : 120,"
                      "\"queue_timeout\" : 60,"
                      "\"task_timeout\" : 15,"
                      "\"max_failed_nodes\" : 10,"
                      "\"num_execution\" : 1,"
                      "\"max_cluster_cpu\" : -1,"
                      "\"max_cpu\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}" ) );
        BOOST_REQUIRE( job );
        jobs.push_back( JobPtr( job ) );
    }
    mgr.PushJobs( jobs );

    list< JobPtr >::iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        JobPtr j;
        BOOST_CHECK( mgr.GetJobById( (*it)->GetJobId(), j ) );
        BOOST_CHECK( (bool)j );
    }
}

BOOST_AUTO_TEST_CASE( job_delete_by_id )
{
    const int numJobs = 10;

    list< JobPtr > jobs;
    for( int i = 0; i < numJobs; ++i )
    {
        Job *job( mgr.CreateJob(
                      "{\"script\" : \"simple.py\","
                      "\"language\" : \"python\","
                      "\"send_script\" : false,"
                      "\"priority\" : 4,"
                      "\"job_timeout\" : 120,"
                      "\"queue_timeout\" : 60,"
                      "\"task_timeout\" : 15,"
                      "\"max_failed_nodes\" : 10,"
                      "\"num_execution\" : 1,"
                      "\"max_cluster_cpu\" : -1,"
                      "\"max_cpu\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}" ) );
        BOOST_REQUIRE( job );
        jobs.push_back( JobPtr( job ) );
    }
    mgr.PushJobs( jobs );

    list< JobPtr >::iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        JobPtr j;
        BOOST_CHECK( mgr.DeleteJob( (*it)->GetJobId() ) );
        BOOST_CHECK_EQUAL( mgr.GetJobById( (*it)->GetJobId(), j ), false );
    }
}

BOOST_AUTO_TEST_CASE( job_group_delete )
{
    const int numJobs = 10;

    list< JobPtr > jobs;
    for( int i = 0; i < numJobs; ++i )
    {
        Job *job( mgr.CreateJob(
                      "{\"script\" : \"simple.py\","
                      "\"language\" : \"python\","
                      "\"send_script\" : false,"
                      "\"priority\" : 4,"
                      "\"job_timeout\" : 120,"
                      "\"queue_timeout\" : 60,"
                      "\"task_timeout\" : 15,"
                      "\"max_failed_nodes\" : 10,"
                      "\"num_execution\" : 1,"
                      "\"max_cluster_cpu\" : -1,"
                      "\"max_cpu\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}" ) );
        BOOST_REQUIRE( job );
        jobs.push_back( JobPtr( job ) );
    }
    mgr.PushJobs( jobs );

    JobPtr j;
    BOOST_REQUIRE( mgr.PopJob( j ) );
    BOOST_REQUIRE( (bool)j );

    mgr.DeleteJobGroup( j->GetGroupId() );

    BOOST_CHECK_EQUAL( mgr.PopJob( j ), false );
}

BOOST_AUTO_TEST_CASE( job_delete_all )
{
    const int numJobs = 10;

    for( int i = 0; i < numJobs; ++i )
    {
        Job *job( mgr.CreateJob(
                      "{\"script\" : \"simple.py\","
                      "\"language\" : \"python\","
                      "\"send_script\" : false,"
                      "\"priority\" : 4,"
                      "\"job_timeout\" : 120,"
                      "\"queue_timeout\" : 60,"
                      "\"task_timeout\" : 15,"
                      "\"max_failed_nodes\" : 10,"
                      "\"num_execution\" : 1,"
                      "\"max_cluster_cpu\" : -1,"
                      "\"max_cpu\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}" ) );
        BOOST_REQUIRE( job );
        mgr.PushJob( job );
    }

    mgr.DeleteAllJobs();
    JobPtr j;
    BOOST_CHECK_EQUAL( mgr.PopJob( j ), false );
}

BOOST_AUTO_TEST_SUITE_END()

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

BOOST_AUTO_TEST_CASE( host_appearance )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    const Scheduler::IPToNodeState &ipToNodeState = sched.GetNodeState();
    Scheduler::IPToNodeState::const_iterator it = ipToNodeState.find( "127.0.0.1" );
    BOOST_REQUIRE( it != ipToNodeState.end() );
    const NodeState &nodeState = it->second;
    BOOST_CHECK_EQUAL( nodeState.GetNumFreeCPU(), 2 );
}

BOOST_AUTO_TEST_SUITE_END()

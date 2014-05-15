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
        string ip = "127.0.0." + boost::lexical_cast<string>( i + 1 );
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
            string ip = "127.0.0." + boost::lexical_cast<string>( i + 1 );
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
        mgr.AddWorkerHost( "grp", string( "host" ) + boost::lexical_cast<string>( i + 1 ) );
    }
    BOOST_CHECK_EQUAL( mgr.GetTotalCPU(), 0 );

    vector< WorkerPtr > workers;
    mgr.GetWorkers( workers );

    int numCPU = 0;
    for( size_t i = 0; i < workers.size(); ++i )
    {
        string ip = "127.0.0." + boost::lexical_cast<string>( i + 1 );
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
        serviceLocator.Register( (master::IJobEventReceiver*)&jobHistory );
    }

    ~JobManagerEnvironment()
    {
        common::ServiceLocator::Instance().UnregisterAll();
    }

    MockTimeoutManager timeoutManager;
    MockJobHistory jobHistory;
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
        JobPtr job( mgr.CreateJob(
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
        JobPtr job( mgr.CreateJob(
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
        jobs.push_back( job );
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
        JobPtr job( mgr.CreateJob(
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
        jobs.push_back( job );
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
        JobPtr job( mgr.CreateJob(
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
        jobs.push_back( job );
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
        JobPtr job( mgr.CreateJob(
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
        jobs.push_back( job );
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
        JobPtr job( mgr.CreateJob(
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

BOOST_AUTO_TEST_CASE( job_priority )
{
    const int numGroups = 5;
    const int numJobs = 10;

    for( int k = 0; k < numGroups; ++k )
    {
        vector< JobPtr > jobs;
        for( int i = 0; i < numJobs; ++i )
        {
            int priority = i % 10;
            JobPtr job( new Job( "", "python", priority, 1, 1, 1, 1,
                                 1, 1, 1, false, false ) );
            BOOST_REQUIRE( job );
            jobs.push_back( job );
        }
        random_shuffle( jobs.begin(), jobs.end() );
        list< JobPtr > jobList;

        for( int i = 0; i < numJobs; ++i )
        {
            jobList.push_back( JobPtr( jobs[i] ) );
        }

        mgr.PushJobs( jobList );
    }

    int lastPriority, lastGroupId;
    for( int i = 0; i < numJobs * numGroups; ++i )
    {
        JobPtr j;
        BOOST_CHECK( mgr.PopJob( j ) );
        BOOST_CHECK( (bool)j );

        if ( i )
        {
            BOOST_CHECK_GE( j->GetPriority(), lastPriority );
            if ( i % numGroups )
                BOOST_CHECK_GE( j->GetGroupId(), lastGroupId );
        }
        lastPriority = j->GetPriority();
        lastGroupId = j->GetGroupId();
    }
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
        serviceLocator.Register( (master::IJobEventReceiver*)&jobHistory );
        serviceLocator.Register( (master::IWorkerManager*)&workerMgr );
    }

    ~SchedulerEnvironment()
    {
        common::ServiceLocator::Instance().UnregisterAll();
    }

    MockTimeoutManager timeoutMgr;
    JobManager jobMgr;
    MockJobHistory jobHistory;
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

BOOST_AUTO_TEST_CASE( delete_worker )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    sched.DeleteWorker( "host1" );

    const Scheduler::IPToNodeState &ipToNodeState = sched.GetNodeState();
    BOOST_CHECK( ipToNodeState.empty() );
}

BOOST_AUTO_TEST_CASE( on_new_job )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    const int numJobs = 5;

    for( int i = 0; i < numJobs; ++i )
    {
        JobPtr job( jobMgr.CreateJob(
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
        jobMgr.PushJob( job ); // implicitly calls OnNewJob()
    }

    const ScheduledJobs &jobs = sched.GetScheduledJobs();
    BOOST_CHECK_EQUAL( jobs.GetNumJobs(), numJobs );
}

BOOST_AUTO_TEST_CASE( get_task_to_send )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK_EQUAL( sched.GetTaskToSend( workerJob, hostIP, spJob ), false );
}

BOOST_AUTO_TEST_CASE( get_task_to_send_multiple )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    const int numJobs = 5;

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", numJobs, 1024 );

    for( int i = 0; i < numJobs; ++i )
    {
        JobPtr job( jobMgr.CreateJob(
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
        jobMgr.PushJob( job );
    }

    for( int i = 0; i < numJobs; ++i )
    {
        WorkerJob workerJob;
        string hostIP;
        JobPtr spJob;

        BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
        BOOST_CHECK( (bool)spJob );
    }
}

BOOST_AUTO_TEST_CASE( task_send_completion )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    sched.OnTaskSendCompletion( false, workerJob, hostIP );

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK_EQUAL( sched.GetTaskToSend( workerJob, hostIP, spJob ), false );

    const FailedWorkers &failed = sched.GetFailedWorkers();
    BOOST_CHECK_EQUAL( failed.GetFailedJobsCnt(), 1 );
}

BOOST_AUTO_TEST_CASE( task_completion_failure )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_REQUIRE_EQUAL( tasks.empty(), false );

    sched.OnTaskCompletion( -1, 10, tasks[0], hostIP ); // -1 means error
    const FailedWorkers &failed = sched.GetFailedWorkers();
    BOOST_CHECK_EQUAL( failed.GetFailedJobsCnt(), 1 );
    BOOST_CHECK_EQUAL( sched.GetNeedReschedule().size(), 1 );
}

BOOST_AUTO_TEST_CASE( task_completion )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_REQUIRE_EQUAL( tasks.empty(), false );

    sched.OnTaskCompletion( 0, 10, tasks[0], hostIP ); // normal completion

    const Scheduler::IPToNodeState &ipToNodeState = sched.GetNodeState();
    Scheduler::IPToNodeState::const_iterator it = ipToNodeState.find( "127.0.0.1" );
    BOOST_REQUIRE( it != ipToNodeState.end() );
    const NodeState &nodeState = it->second;
    BOOST_CHECK_EQUAL( nodeState.GetNumFreeCPU(), 2 );

    const FailedWorkers &failed = sched.GetFailedWorkers();
    BOOST_CHECK_EQUAL( failed.GetFailedJobsCnt(), 0 );
    BOOST_CHECK( sched.GetNeedReschedule().empty() );
}

BOOST_AUTO_TEST_CASE( task_completion_reschedule )
{
    workerMgr.AddWorkerHost( "grp", "host1" );
    workerMgr.AddWorkerHost( "grp", "host2" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 2 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );
    workerMgr.SetWorkerIP( workers[1], "127.0.0.2" );
    workerMgr.OnNodePingResponse( "127.0.0.2", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP, hostIP2;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

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

    BOOST_CHECK_EQUAL( sched.GetTaskToSend( workerJob, hostIP2, spJob ), false ); // no more avail workers
}

BOOST_AUTO_TEST_CASE( task_completion_reschedule_exclusive )
{
    workerMgr.AddWorkerHost( "grp", "host1" );
    workerMgr.AddWorkerHost( "grp", "host2" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 2 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );
    workerMgr.SetWorkerIP( workers[1], "127.0.0.2" );
    workerMgr.OnNodePingResponse( "127.0.0.2", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
                  "\"exclusive\" : true,"
                  "\"no_reschedule\" : false}" ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP, hostIP2;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) ); // 1st job on 1st worker
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_REQUIRE_EQUAL( tasks.empty(), false );

    sched.OnTaskCompletion( -1, 10, tasks[0], hostIP ); // task failed

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP2, spJob ) ); // 1st job on 2nd worker
    BOOST_CHECK( (bool)spJob );
    BOOST_CHECK_NE( hostIP, hostIP2 );

    job.reset( jobMgr.CreateJob(
                  "{\"script\" : \"simple.py\","
                  "\"language\" : \"python\","
                  "\"send_script\" : false,"
                  "\"priority\" : 4,"
                  "\"job_timeout\" : 120,"
                  "\"queue_timeout\" : 60,"
                  "\"task_timeout\" : 15,"
                  "\"max_failed_nodes\" : 10,"
                  "\"num_execution\" : 10,"
                  "\"max_cluster_cpu\" : -1,"
                  "\"max_cpu\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}" ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP2, spJob ) ); // 2nd job on 1st worker
    BOOST_CHECK( (bool)spJob );
    BOOST_CHECK_EQUAL( hostIP, hostIP2 );

    workerJob.Reset();
    spJob.reset();

    // couldn't execute 2nd job on 2nd worker (exclusive job executing on 2nd worker)
    BOOST_CHECK_EQUAL( sched.GetTaskToSend( workerJob, hostIP2, spJob ), false );
}

BOOST_AUTO_TEST_CASE( task_completion_no_reschedule )
{
    workerMgr.AddWorkerHost( "grp", "host1" );
    workerMgr.AddWorkerHost( "grp", "host2" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 2 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );
    workerMgr.SetWorkerIP( workers[1], "127.0.0.2" );
    workerMgr.OnNodePingResponse( "127.0.0.2", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
                  "\"no_reschedule\" : true}" ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP, hostIP2;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_REQUIRE_EQUAL( tasks.empty(), false );

    sched.OnTaskCompletion( -1, 10, tasks[0], hostIP ); // task failed

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK_EQUAL( sched.GetTaskToSend( workerJob, hostIP2, spJob ), false );
    BOOST_CHECK_EQUAL( sched.GetFailedWorkers().GetFailedJobsCnt(), 0 );
    BOOST_CHECK_EQUAL( sched.GetNeedReschedule().empty(), true );
    BOOST_CHECK_EQUAL( sched.GetScheduledJobs().GetNumJobs(), 0 );
}

BOOST_AUTO_TEST_CASE( task_completion_max_failed_nodes )
{
    workerMgr.AddWorkerHost( "grp", "host1" );
    workerMgr.AddWorkerHost( "grp", "host2" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 2 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );
    workerMgr.SetWorkerIP( workers[1], "127.0.0.2" );
    workerMgr.OnNodePingResponse( "127.0.0.2", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
                  "{\"script\" : \"simple.py\","
                  "\"language\" : \"python\","
                  "\"send_script\" : false,"
                  "\"priority\" : 4,"
                  "\"job_timeout\" : 120,"
                  "\"queue_timeout\" : 60,"
                  "\"task_timeout\" : 15,"
                  "\"max_failed_nodes\" : 2,"
                  "\"num_execution\" : 1,"
                  "\"max_cluster_cpu\" : -1,"
                  "\"max_cpu\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}" ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP, hostIP2;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_REQUIRE_EQUAL( tasks.empty(), false );

    sched.OnTaskCompletion( -1, 10, tasks[0], hostIP ); // task failed

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP2, spJob ) );
    BOOST_CHECK( (bool)spJob );
    BOOST_CHECK_NE( hostIP, hostIP2 );

    sched.OnTaskCompletion( -1, 10, tasks[0], hostIP2 ); // task failed once again => remove job

    BOOST_CHECK_EQUAL( sched.GetFailedWorkers().GetFailedJobsCnt(), 0 );
    BOOST_CHECK_EQUAL( sched.GetNeedReschedule().empty(), true );
    BOOST_CHECK_EQUAL( sched.GetScheduledJobs().GetNumJobs(), 0 );
}

BOOST_AUTO_TEST_CASE( task_completion_num_executions )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
                  "{\"script\" : \"simple.py\","
                  "\"language\" : \"python\","
                  "\"send_script\" : false,"
                  "\"priority\" : 4,"
                  "\"job_timeout\" : 120,"
                  "\"queue_timeout\" : 60,"
                  "\"task_timeout\" : 15,"
                  "\"max_failed_nodes\" : 10,"
                  "\"num_execution\" : 5,"
                  "\"max_cluster_cpu\" : -1,"
                  "\"max_cpu\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}" ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    const int numExec = 5;
    for( int i = 0; i < numExec; ++i )
    {
        WorkerJob workerJob;
        string hostIP;
        JobPtr spJob;

        if ( !sched.GetTaskToSend( workerJob, hostIP, spJob ) )
            break;
        BOOST_CHECK( (bool)spJob );

        vector< WorkerTask > tasks;
        workerJob.GetTasks( tasks );
        BOOST_CHECK_EQUAL( tasks.empty(), false );

        vector< WorkerTask >::const_iterator it = tasks.begin();
        for( ; it != tasks.end(); ++it )
        {
            sched.OnTaskCompletion( 0, 10, *it, hostIP );
        }
    }

    BOOST_CHECK_EQUAL( sched.GetScheduledJobs().GetNumJobs(), 0 );
}

BOOST_AUTO_TEST_CASE( task_completion_max_cluster_cpu )
{
    workerMgr.AddWorkerHost( "grp", "host1" );
    workerMgr.AddWorkerHost( "grp", "host2" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 2 );

    const int numCPU = 4;
    const int maxClusterCPU = 5;
    BOOST_REQUIRE_GT( maxClusterCPU, numCPU );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", numCPU, 1024 );
    workerMgr.SetWorkerIP( workers[1], "127.0.0.2" );
    workerMgr.OnNodePingResponse( "127.0.0.2", numCPU, 1024 );

    JobPtr job( jobMgr.CreateJob(
                  "{\"script\" : \"simple.py\","
                  "\"language\" : \"python\","
                  "\"send_script\" : false,"
                  "\"priority\" : 4,"
                  "\"job_timeout\" : 120,"
                  "\"queue_timeout\" : 60,"
                  "\"task_timeout\" : 15,"
                  "\"max_failed_nodes\" : 10,"
                  "\"num_execution\" : 10,"
                  "\"max_cluster_cpu\" : 5,"
                  "\"max_cpu\" : 4,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}" ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP, hostIP2;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_CHECK_EQUAL( tasks.size(), numCPU );

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP2, spJob ) );
    BOOST_CHECK( (bool)spJob );
    BOOST_CHECK_NE( hostIP, hostIP2 );

    tasks.clear();
    workerJob.GetTasks( tasks );
    BOOST_CHECK_EQUAL( tasks.size(), maxClusterCPU - numCPU );
}

BOOST_AUTO_TEST_CASE( task_completion_max_cpu )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    const int numCPU = 8;

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", numCPU, 1024 );

    JobPtr job( jobMgr.CreateJob(
                  "{\"script\" : \"simple.py\","
                  "\"language\" : \"python\","
                  "\"send_script\" : false,"
                  "\"priority\" : 4,"
                  "\"job_timeout\" : 120,"
                  "\"queue_timeout\" : 60,"
                  "\"task_timeout\" : 15,"
                  "\"max_failed_nodes\" : 10,"
                  "\"num_execution\" : 5,"
                  "\"max_cluster_cpu\" : -1,"
                  "\"max_cpu\" : 4,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}" ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    const int maxCPU = 4;
    BOOST_REQUIRE_GT( numCPU, maxCPU );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_CHECK_EQUAL( tasks.size(), maxCPU );
}

BOOST_AUTO_TEST_CASE( task_completion_exclusive ) // 1st job - exclusive, 2nd job - not exclusive
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
                  "\"exclusive\" : true,"
                  "\"no_reschedule\" : false}" ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    job.reset( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK_EQUAL( sched.GetTaskToSend( workerJob, hostIP, spJob ), false );
}

BOOST_AUTO_TEST_CASE( task_completion_exclusive2 ) // 1st job - not exclusive, 2nd job - exclusive
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    job.reset( jobMgr.CreateJob(
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
                  "\"exclusive\" : true,"
                  "\"no_reschedule\" : false}" ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK_EQUAL( sched.GetTaskToSend( workerJob, hostIP, spJob ), false );
}

BOOST_AUTO_TEST_CASE( task_completion_exclusive3 ) // 1st _completed_ job - exclusive, 2nd job - exclusive
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
                  "\"exclusive\" : true,"
                  "\"no_reschedule\" : false}" ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_CHECK_EQUAL( tasks.empty(), false );

    vector< WorkerTask >::const_iterator it = tasks.begin();
    for( ; it != tasks.end(); ++it )
    {
        sched.OnTaskCompletion( 0, 10, *it, hostIP );
    }

    job.reset( jobMgr.CreateJob(
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
                  "\"exclusive\" : true,"
                  "\"no_reschedule\" : false}" ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );
}

BOOST_AUTO_TEST_CASE( task_completion_delete_worker )
{
    workerMgr.AddWorkerHost( "grp", "host1" );
    workerMgr.AddWorkerHost( "grp", "host2" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 2 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );
    workerMgr.SetWorkerIP( workers[1], "127.0.0.2" );
    workerMgr.OnNodePingResponse( "127.0.0.2", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP, hostIP2;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    sched.DeleteWorker( workers[0]->GetHost() );

    workerJob.Reset();
    spJob.reset();

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP2, spJob ) );
    BOOST_CHECK( (bool)spJob );
    BOOST_CHECK_NE( hostIP, hostIP2 );
}

BOOST_AUTO_TEST_CASE( on_task_timeout )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_CHECK_EQUAL( tasks.empty(), false );
    vector< WorkerTask >::const_iterator it = tasks.begin();
    for( ; it != tasks.end(); ++it )
    {
        sched.OnTaskTimeout( *it, hostIP );
    }

    BOOST_CHECK_EQUAL( sched.GetNeedReschedule().size(), tasks.size() );
}

BOOST_AUTO_TEST_CASE( on_task_timeout_after_completion )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_CHECK_EQUAL( tasks.empty(), false );
    vector< WorkerTask >::const_iterator it = tasks.begin();
    for( ; it != tasks.end(); ++it )
    {
        sched.OnTaskCompletion( 0, 10, *it, hostIP );
    }

    for( it = tasks.begin(); it != tasks.end(); ++it )
    {
        sched.OnTaskTimeout( *it, hostIP );
    }

    BOOST_CHECK( sched.GetNeedReschedule().empty() );
}

BOOST_AUTO_TEST_CASE( on_job_timeout )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    BOOST_CHECK_GT( sched.GetScheduledJobs().GetNumJobs(), 0 );
    sched.OnJobTimeout( workerJob.GetJobId() );
    BOOST_CHECK_EQUAL( sched.GetScheduledJobs().GetNumJobs(), 0 );
}

BOOST_AUTO_TEST_CASE( stop_job )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 2, 1024 );

    JobPtr job( jobMgr.CreateJob(
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
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    BOOST_CHECK_GT( sched.GetScheduledJobs().GetNumJobs(), 0 );
    sched.StopJob( workerJob.GetJobId() );
    BOOST_CHECK_EQUAL( sched.GetScheduledJobs().GetNumJobs(), 0 );
}

BOOST_AUTO_TEST_CASE( stop_all_jobs )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    const int numJobs = 5;

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", numJobs, 1024 );

    for( int i = 0; i < numJobs; ++i )
    {
        JobPtr job( jobMgr.CreateJob(
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
        jobMgr.PushJob( job );
    }

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    BOOST_CHECK_EQUAL( sched.GetScheduledJobs().GetNumJobs(), numJobs );
    sched.StopAllJobs();
    BOOST_CHECK_EQUAL( sched.GetScheduledJobs().GetNumJobs(), 0 );
}

BOOST_AUTO_TEST_SUITE_END()

////////////////////////////////////////////////////////////////
// ScheduledJobs
////////////////////////////////////////////////////////////////

BOOST_FIXTURE_TEST_SUITE( ScheduledJobsSuite, ScheduledJobs )

BOOST_AUTO_TEST_CASE( jobs_insertion )
{
    const int numJobs = 20;

    vector< JobPtr > jobs;
    for( int i = 0; i < numJobs; ++i )
    {
        int priority = i % 10;
        JobPtr job( new Job( "", "python", priority, 1, 1, 1, 1,
                             1, 1, 1, false, false ) );
        jobs.push_back( job );
        Add( job, i + 1 );
    }

    vector< JobPtr >::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        JobPtr j;
        BOOST_CHECK( FindJobByJobId( (*it)->GetJobId(), j ) );
    }
}

BOOST_AUTO_TEST_CASE( jobs_removal )
{
    const int numJobs = 20;

    vector< JobPtr > jobs;
    for( int i = 0; i < numJobs; ++i )
    {
        int priority = i % 10;
        JobPtr job( new Job( "", "python", priority, 1, 1, 1, 1,
                             1, 1, 1, false, false ) );
        job->SetJobId( i );
        jobs.push_back( job );
        Add( job, i + 1 );
    }

    vector< JobPtr >::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        RemoveJob( (*it)->GetJobId(), "test" );
        JobPtr j;
        BOOST_CHECK_EQUAL( FindJobByJobId( (*it)->GetJobId(), j ), false );
    }
}

BOOST_AUTO_TEST_CASE( jobs_decrement_execution )
{
    const int numExec = 10;

    JobPtr job( new Job( "", "python", 4, 1, 1, 1, 1,
                         1, 1, 1, false, false ) );
    Add( job, numExec );
    JobPtr j;
    BOOST_CHECK( FindJobByJobId( job->GetJobId(), j ) );

    for( int i = 0; i < numExec; ++i )
    {
        DecrementJobExecution( job->GetJobId(), 1 );
    }
    BOOST_CHECK_EQUAL( FindJobByJobId( job->GetJobId(), j ), false );
}

BOOST_AUTO_TEST_CASE( jobs_get_group )
{
    const int numJobs = 10;

    for( int i = 0; i < numJobs; ++i )
    {
        JobPtr job( new Job( "", "python", 4, 1, 1, 1, 1,
                             1, 1, 1, false, false ) );
        job->SetGroupId( i % 2 );
        Add( job, i + 1 );
    }

    std::list< JobPtr > jobs;
    GetJobGroup( 0, jobs );
    BOOST_CHECK_EQUAL( jobs.size(), numJobs / 2 );
}

BOOST_AUTO_TEST_CASE( jobs_priority )
{
    const int numGroups = 5;
    const int numJobs = 10;

    for( int k = 0; k < numGroups; ++k )
    {
        for( int i = 0; i < numJobs; ++i )
        {
            int priority = i % 10;
            JobPtr job( new Job( "", "python", priority, 1, 1, 1, 1,
                                 1, 1, 1, false, false ) );
            job->SetGroupId( k );
            Add( job, i + 1 );
        }
    }

    const ScheduledJobs::JobQueue &jobs = GetJobQueue();
    ScheduledJobs::JobQueue::const_iterator it = jobs.begin();

    int lastPriority, lastGroupId;
    for( int i = 0; it != jobs.end(); ++it, ++i )
    {
        const JobPtr &j = (*it).GetJob();
        if ( i )
        {
            BOOST_CHECK_GE( j->GetPriority(), lastPriority );
            if ( i % numGroups )
                BOOST_CHECK_GE( j->GetGroupId(), lastGroupId );
        }
        lastPriority = j->GetPriority();
        lastGroupId = j->GetGroupId();
    }
}

BOOST_AUTO_TEST_SUITE_END()

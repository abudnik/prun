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
    auto it = ipToNodeState.find( "127.0.0.1" );
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
                      "\"max_cluster_instances\" : -1,"
                      "\"max_worker_instances\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                      "\"max_cluster_instances\" : -1,"
                      "\"max_worker_instances\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
    auto it = ipToNodeState.find( "127.0.0.1" );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : true,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : true}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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

BOOST_AUTO_TEST_CASE( task_completion_num_executions_no_limit_cpu )
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
                  "\"num_execution\" : -1,"
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : -1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    sched.GetTaskToSend( workerJob, hostIP, spJob );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_CHECK_EQUAL( tasks.size(), numCPU );
}

BOOST_AUTO_TEST_CASE( task_completion_num_executions_no_limit_host )
{
    workerMgr.AddWorkerHost( "grp", "host1" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), 1 );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 8, 1024 );

    JobPtr job( jobMgr.CreateJob(
                  "{\"script\" : \"simple.py\","
                  "\"language\" : \"python\","
                  "\"send_script\" : false,"
                  "\"priority\" : 4,"
                  "\"job_timeout\" : 120,"
                  "\"queue_timeout\" : 60,"
                  "\"task_timeout\" : 15,"
                  "\"max_failed_nodes\" : 10,"
                  "\"num_execution\" : -1,"
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : -1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false,"
                  "\"exec_unit_type\" : \"host\"}", true ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    sched.GetTaskToSend( workerJob, hostIP, spJob );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_CHECK_EQUAL( tasks.size(), 1 );
}

BOOST_AUTO_TEST_CASE( task_completion_num_executions_limit_hosts )
{
    const size_t numHosts = 2;

    workerMgr.AddWorkerHost( "grp", "host1" );
    workerMgr.AddWorkerHost( "grp", "host2" );

    vector< WorkerPtr > workers;
    workerMgr.GetWorkers( workers );
    BOOST_REQUIRE_EQUAL( workers.size(), numHosts );

    workerMgr.SetWorkerIP( workers[0], "127.0.0.1" );
    workerMgr.OnNodePingResponse( "127.0.0.1", 8, 1024 );

    workerMgr.SetWorkerIP( workers[1], "127.0.0.2" );
    workerMgr.OnNodePingResponse( "127.0.0.2", 8, 1024 );

    JobPtr job( jobMgr.CreateJob(
                  "{\"script\" : \"simple.py\","
                  "\"language\" : \"python\","
                  "\"send_script\" : false,"
                  "\"priority\" : 4,"
                  "\"job_timeout\" : 120,"
                  "\"queue_timeout\" : 60,"
                  "\"task_timeout\" : 15,"
                  "\"max_failed_nodes\" : 10,"
                  "\"num_execution\" : -1,"
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : -1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false,"
                  "\"max_exec_at_worker\" : 1,"
                  "\"exec_unit_type\" : \"host\","
                  "\"hosts\" : [\"host1\", \"host2\"]}", true ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    size_t totalTasks = 0;
    for( size_t i = 0; i < numHosts; ++i )
    {
        WorkerJob workerJob;
        string hostIP;
        JobPtr spJob;

        sched.GetTaskToSend( workerJob, hostIP, spJob );
        BOOST_REQUIRE( (bool)spJob );

        vector< WorkerTask > tasks;
        workerJob.GetTasks( tasks );
        totalTasks += tasks.size();
    }

    BOOST_REQUIRE_EQUAL( totalTasks, numHosts );
}

BOOST_AUTO_TEST_CASE( task_completion_max_cluster_instances )
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
                  "\"max_cluster_instances\" : 5,"
                  "\"max_worker_instances\" : 4,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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

BOOST_AUTO_TEST_CASE( task_completion_max_worker_instances )
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 4,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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

BOOST_AUTO_TEST_CASE( task_completion_max_exec_at_worker )
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 4,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false,"
                  "\"max_exec_at_worker\" : 4}", true ) );
    BOOST_REQUIRE( job );
    jobMgr.PushJob( job );

    const int maxExecAtWorker = 4;
    BOOST_REQUIRE_GT( numCPU, maxExecAtWorker );

    WorkerJob workerJob;
    string hostIP;
    JobPtr spJob;

    BOOST_CHECK( sched.GetTaskToSend( workerJob, hostIP, spJob ) );
    BOOST_CHECK( (bool)spJob );

    vector< WorkerTask > tasks;
    workerJob.GetTasks( tasks );
    BOOST_CHECK_EQUAL( tasks.size(), maxExecAtWorker );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : true,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : true,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : true,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : true,"
                  "\"no_reschedule\" : false}", true ) );
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
    workerMgr.OnNodePingResponse( "127.0.0.1", 4, 1024 );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                  "\"max_cluster_instances\" : -1,"
                  "\"max_worker_instances\" : 1,"
                  "\"exclusive\" : false,"
                  "\"no_reschedule\" : false}", true ) );
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
                      "\"max_cluster_instances\" : -1,"
                      "\"max_worker_instances\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}", true ) );
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

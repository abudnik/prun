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
                     "\"max_cluster_instances\" : -1,"
                     "\"max_worker_instances\" : 1,"
                     "\"exclusive\" : false,"
                     "\"no_reschedule\" : false}", true ) );
    BOOST_CHECK( (bool)job1 );

    JobPtr job2( mgr.CreateJob( "_garbage_", true ) );
    BOOST_CHECK_EQUAL( (bool)job2, false );

    JobPtr job3( mgr.CreateJob( "{\"_random_field_\" : 1}", true ) );
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
                      "\"max_cluster_instances\" : -1,"
                      "\"max_worker_instances\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}", true ) );
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
                      "\"max_cluster_instances\" : -1,"
                      "\"max_worker_instances\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}", true ) );
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
                      "\"max_cluster_instances\" : -1,"
                      "\"max_worker_instances\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}", true ) );
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
                      "\"max_cluster_instances\" : -1,"
                      "\"max_worker_instances\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}", true ) );
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
                      "\"max_cluster_instances\" : -1,"
                      "\"max_worker_instances\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}", true ) );
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
                      "\"max_cluster_instances\" : -1,"
                      "\"max_worker_instances\" : 1,"
                      "\"exclusive\" : false,"
                      "\"no_reschedule\" : false}", true ) );
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
            jobList.push_back( jobs[i] );
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

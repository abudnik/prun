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
        serviceLocator.Register( (master::IJobManager*)&mgr );
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

BOOST_AUTO_TEST_CASE( test_job_name_registry )
{
    std::string jobName1 = "jobName1";
    std::string jobName2 = "jobName2";

    BOOST_REQUIRE( mgr.RegisterJobName( jobName1 ) );
    BOOST_REQUIRE( !mgr.RegisterJobName( jobName1 ) );
    BOOST_REQUIRE( mgr.RegisterJobName( jobName2 ) );
    BOOST_REQUIRE( !mgr.RegisterJobName( jobName2 ) );

    BOOST_REQUIRE( mgr.ReleaseJobName( jobName2 ) );
    BOOST_REQUIRE( !mgr.ReleaseJobName( jobName2 ) );

    BOOST_REQUIRE( !mgr.RegisterJobName( "" ) );
    BOOST_REQUIRE( !mgr.ReleaseJobName( "empty" ) );
}

BOOST_AUTO_TEST_CASE( test_named_job_uniqueness )
{
    const int numJobs = 10;

    for( int i = 0; i < 2; ++i )
    {
        for( int j = 0; j < numJobs; ++j )
        {
            std::string jobDescr =
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
                "\"no_reschedule\" : false,"
                "\"name\" : \"";
            jobDescr += "exclusive_name_" + std::to_string( j ) + "\"}";

            JobPtr job( mgr.CreateJob( jobDescr, true ) );
            if ( i == 0 )
            {
                BOOST_REQUIRE( job );
                mgr.PushJob( job );
            }
            else
            {
                BOOST_REQUIRE( !job );
            }
        }
    }
}

BOOST_AUTO_TEST_CASE( test_cron_job_creation )
{
    std::string validJob =
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
        "\"no_reschedule\" : false,"
        "\"cron\" : \"*/5 * * * 0\","
        "\"name\" : \"job_name\"}";

    JobPtr j( mgr.CreateJob( validJob, true ) );
    BOOST_REQUIRE( mgr.RegisterJobName( "job_name" ) );
    BOOST_REQUIRE( j );

    JobPtr j2( mgr.CreateJob( validJob, true ) );
    BOOST_REQUIRE( !j2 );

    const bool check_name_existance = false;
    JobPtr j3( mgr.CreateJob( validJob, check_name_existance ) );
    BOOST_REQUIRE( j3 );

    std::string cronJobNoName =
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
        "\"no_reschedule\" : false,"
        "\"cron\" : \"*/5 * * * 0\"}";

    JobPtr j4( mgr.CreateJob( cronJobNoName, true ) );
    BOOST_REQUIRE( !j4 );
}

BOOST_AUTO_TEST_CASE( test_cron_job_removal )
{
    const int numJobs = 10;

    for( int i = 0; i < 2; ++i )
    {
        for( int j = 0; j < numJobs; ++j )
        {
            std::string jobName = "exclusive_name_" + std::to_string( j );
            std::string jobDescr =
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
                "\"no_reschedule\" : false,"
                "\"name\" : \"";
            jobDescr += jobName + "\"}";

            JobPtr job( mgr.CreateJob( jobDescr, true ) );
            if ( i == 0 )
            {
                BOOST_REQUIRE( job );
                mgr.PushJob( job );
            }
            else
            {
                BOOST_REQUIRE( mgr.DeleteNamedJob( jobName ) );
            }
        }
    }

    JobPtr j;
    BOOST_REQUIRE( !mgr.PopJob( j ) );
}

BOOST_AUTO_TEST_SUITE_END()

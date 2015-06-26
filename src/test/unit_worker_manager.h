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
        string ip = "127.0.0." + std::to_string( i + 1 );
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
        serviceLocator.Register( (master::IWorkerManager*)&mgr );
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
            string ip = "127.0.0." + std::to_string( i + 1 );
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
        mgr.AddWorkerHost( "grp", string( "host" ) + std::to_string( i + 1 ) );
    }
    BOOST_CHECK_EQUAL( mgr.GetTotalCPU(), 0 );

    vector< WorkerPtr > workers;
    mgr.GetWorkers( workers );

    int numCPU = 0;
    for( size_t i = 0; i < workers.size(); ++i )
    {
        string ip = "127.0.0." + std::to_string( i + 1 );
        mgr.SetWorkerIP( workers[i], ip );
        mgr.OnNodePingResponse( ip, i + 1, 1024 );
        numCPU += i + 1;
    }
    BOOST_CHECK_GT( mgr.GetTotalCPU(), 0 );
    BOOST_CHECK_EQUAL( mgr.GetTotalCPU(), numCPU );
}

BOOST_AUTO_TEST_SUITE_END()

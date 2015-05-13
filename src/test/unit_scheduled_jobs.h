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

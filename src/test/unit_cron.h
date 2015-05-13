////////////////////////////////////////////////////////////////
// Cron
////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_CASE( cron_parser )
{
    common::CronJob j1, j2, j3, j4, j5, j6, j7, j8, j9;
    BOOST_CHECK( j1.Parse( "  */5 1 */5 1-10/3 */7" ) );
    BOOST_CHECK( j2.Parse( "1-5/2,8-12/2,15,23 2 */5 */6 */7 " ) );
    BOOST_CHECK( j3.Parse( "1,2,3,4-5/1,6 3 * */1 0-5" ) );
    BOOST_CHECK( j4.Parse( "1 4 * */1 */0" ) == false );
    BOOST_CHECK( j5.Parse( "invalid 5 3 4 5" ) == false );
    BOOST_CHECK( j6.Parse( "5-1 6 1-5 * *" ) );
    BOOST_CHECK( j7.Parse( "1,2,500 7 * * *" ) == false );
    BOOST_CHECK( j8.Parse( "* * * * *" ) );
    BOOST_CHECK( j9.Parse( "" ) == false );
}

BOOST_AUTO_TEST_CASE( cron_scheduler )
{
    using std::chrono::system_clock;

    common::CronJob j1, j2, j3;
    BOOST_CHECK( j1.Parse( "* * * * *" ) );

    auto d1 = system_clock::from_time_t( common::CreateDateTime( 2020, 12, 31, 23, 59 ) );
    auto d2 = system_clock::from_time_t( common::CreateDateTime( 2021, 1, 1, 0, 0 ) );
    BOOST_CHECK( j1.Next( d1 ) == d1 );
    BOOST_CHECK( j1.Next( d2 ) == d2 );

    BOOST_CHECK( j2.Parse( "* * * 1 *" ) );
    BOOST_CHECK( j2.Next( d1 ) == d2 );

    BOOST_CHECK( j3.Parse( "1-59/2 3 27 6 *" ) );
    auto d3 = system_clock::from_time_t( common::CreateDateTime( 2021, 6, 27, 3, 1 ) );

    BOOST_CHECK( j3.Next( d1 ) == d3 );
    BOOST_CHECK( j3.Next( d2 ) == d3 );
}

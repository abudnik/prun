#define BOOST_SPIRIT_THREADSAFE

#include <boost/property_tree/json_parser.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/graph/depth_first_search.hpp>
#include <boost/graph/visitors.hpp>
#include <iterator>
#include "job_manager.h"
#include "common/log.h"
#include "common/config.h"
#include "common/helper.h"
#include "scheduler.h"
#include "timeout_manager.h"

namespace boost {

struct cycle_detector : public dfs_visitor<>
{
    cycle_detector( bool &has_cycle )
    : has_cycle_( has_cycle ) {}

    template< class Edge, class Graph >
    void back_edge( Edge, Graph & ) { has_cycle_ = true; }
private:
    bool &has_cycle_;
};

} // namespace boost

namespace master {

bool JDLJason::ParseJob( const std::string &job_description, boost::property_tree::ptree &ptree )
{
    std::istringstream ss( job_description );
    try
    {
        boost::property_tree::read_json( ss, ptree );
    }
    catch( boost::property_tree::json_parser::json_parser_error &e )
    {
        PS_LOG( "JDLJason::ParseJob read_json failed: " << e.what() );
        return false;
    }
    return true;
}

int64_t JobManager::numJobGroups_ = 0;

Job *JobManager::CreateJob( const std::string &job_description ) const
{
    boost::property_tree::ptree ptree;
    JDLJason parser;
    if ( !parser.ParseJob( job_description, ptree ) )
        return NULL;

    return CreateJob( ptree );
}

void JobManager::CreateMetaJob( const std::string &meta_description, std::list< Job * > &jobs ) const
{
    std::istringstream ss( meta_description );
    std::string line;
    typedef std::set< std::string > StringSet;
    StringSet jobFiles;

    // read job description file pathes
    std::copy( std::istream_iterator<std::string>( ss ),
               std::istream_iterator<std::string>(),
               std::inserter< std::set< std::string > >( jobFiles, jobFiles.begin() ) );

    int index = 0;
    std::map< std::string, int > jobFileToIndex;

    boost::shared_ptr< JobGroup > jobGroup( new JobGroup() );
    std::vector< Job * > &indexToJob = jobGroup->GetIndexToJob();

    // parse job files 
    bool succeeded = true;
    StringSet::const_iterator it = jobFiles.begin();
    for( ; it != jobFiles.end(); ++it )
    {
        // read job description from file
        std::string filePath = exeDir_ + '/' + *it;
        std::ifstream file( filePath.c_str() );
        if ( !file.is_open() )
        {
            PS_LOG( "CreateMetaJob: couldn't open " << filePath );
            succeeded = false;
            break;
        }
        std::string jobDescr;
        while( getline( file, line ) )
            jobDescr += line;

        Job *job = CreateJob( jobDescr );
        if ( job )
        {
            jobFileToIndex[ *it ] = index++;
            indexToJob.push_back( job );
            jobs.push_back( job );
        }
        else
        {
            PS_LOG( "JobManager::CreateMetaJob: CreateJob failed, job=" << *it );
            succeeded = false;
            break;
        }
    }
    if ( succeeded )
    {
        succeeded = PrepareJobGraph( ss, jobFileToIndex, jobGroup );
    }

    if ( !succeeded )
    {
        std::list< Job * >::iterator it = jobs.begin();
        for( ; it != jobs.end(); )
        {
            delete *it;
            jobs.erase( it++ );
        }
    }
}

void JobManager::PushJob( Job *job )
{
    PS_LOG( "push job" );
    jobs_.PushJob( job, numJobGroups_++ );

    Scheduler::Instance().OnNewJob();
    timeoutManager_->PushJobQueue( job->GetJobId(), job->GetQueueTimeout() );
}

void JobManager::PushJobs( std::list< Job * > &jobs )
{
    PS_LOG( "push jobs" );
    jobs_.PushJobs( jobs, numJobGroups_++ );

    Scheduler::Instance().OnNewJob();

    std::list< Job * >::const_iterator it = jobs.begin();
    for( ; it != jobs.end(); ++it )
    {
        const Job *job = *it;
        timeoutManager_->PushJobQueue( job->GetJobId(), job->GetQueueTimeout() );
    }
}

Job *JobManager::GetJobById( int64_t jobId )
{
    return jobs_.GetJobById( jobId );
}

bool JobManager::DeleteJob( int64_t jobId )
{
    return jobs_.DeleteJob( jobId );
}

bool JobManager::DeleteJobGroup( int64_t groupId )
{
    return jobs_.DeleteJobGroup( groupId );
}

Job *JobManager::PopJob()
{
    return jobs_.PopJob();
}

Job *JobManager::GetTopJob()
{
    return jobs_.GetTopJob();
}

void JobManager::Initialize( const std::string &masterId, const std::string &exeDir, TimeoutManager *timeoutManager )
{
    masterId_ = masterId;
    exeDir_ = exeDir;
    timeoutManager_ = timeoutManager;
}

void JobManager::Shutdown()
{
    jobs_.Clear();
}

bool JobManager::ReadScript( const std::string &fileName, std::string &script ) const
{
    std::string filePath = exeDir_ + '/' + fileName;
    std::ifstream file( filePath.c_str() );
    if ( !file.is_open() )
    {
        PS_LOG( "JobManager::ReadScript: couldn't open " << filePath );
        return false;
    }

    std::string data;
    file.seekg( 0, std::ios::end );
    data.resize( file.tellg() );
    file.seekg( 0, std::ios::beg );
    file.read( &data[0], data.size() );

    return common::EncodeBase64( data.c_str(), data.size(), script );
}

Job *JobManager::CreateJob( const boost::property_tree::ptree &ptree ) const
{
    try
    {
        std::string fileName = ptree.get<std::string>( "script" );
        if ( fileName.empty() )
        {
            PS_LOG( "JobManager::CreateJob: empty script file name" );
            return NULL;
        }

        bool sendScript = ptree.get<bool>( "send_script" );

        std::string script;
        if ( sendScript && !ReadScript( fileName, script ) )
            return NULL;

        std::string language = ptree.get<std::string>( "language" );
        int priority = ptree.get<int>( "priority" );
        int timeout = ptree.get<int>( "job_timeout" );
        int queueTimeout = ptree.get<int>( "queue_timeout" );
        int taskTimeout = ptree.get<int>( "task_timeout" );
        int maxFailedNodes = ptree.get<int>( "max_failed_nodes" );
        int numExec = ptree.get<int>( "num_execution" );
        int maxClusterCPU = ptree.get<int>( "max_cluster_cpu" );
        int maxCPU = ptree.get<int>( "max_cpu" );
        bool exclusive = ptree.get<bool>( "exclusive" );
        bool noReschedule = ptree.get<bool>( "no_reschedule" );

        if ( taskTimeout < 0 )
            taskTimeout = -1;

        Job *job = new Job( script, language,
                            priority, maxFailedNodes,
                            numExec, maxClusterCPU, maxCPU,
                            timeout, queueTimeout, taskTimeout,
                            exclusive, noReschedule );

        if ( !sendScript )
            job->SetFilePath( fileName );

        if ( ptree.count( "hosts" ) > 0 )
        {
            ReadHosts( job, ptree );
        }

        return job;
    }
    catch( std::exception &e )
    {
        PS_LOG( "JobManager::CreateJob exception: " << e.what() );
        return NULL;
    }
}

void JobManager::ReadHosts( Job *job, const boost::property_tree::ptree &ptree ) const
{
    using boost::asio::ip::udp;
    udp::resolver resolver( io_service_ );

    common::Config &cfg = common::Config::Instance();
    bool ipv6 = cfg.Get<bool>( "ipv6" );

    BOOST_FOREACH( const boost::property_tree::ptree::value_type &v,
                   ptree.get_child( "hosts" ) )
    {
        std::string host = v.second.get_value< std::string >();
        udp::resolver::query query( ipv6 ? udp::v6() : udp::v4(), host, "" );

        boost::system::error_code error;
        udp::resolver::iterator iter = resolver.resolve( query, error ), end;
        if ( error || iter == end )
        {
            PS_LOG( "JobManager::ReadHosts: address not resolved '" << host << "'" );
            continue;
        }

        udp::endpoint dest = *iter;
        std::string ip = dest.address().to_string();
        job->AddHost( ip );
    }
}

bool JobManager::PrepareJobGraph( std::istringstream &ss,
                                  std::map< std::string, int > &jobFileToIndex,
                                  boost::shared_ptr< JobGroup > &jobGroup ) const
{
    using namespace boost;

    // create graph
    JobGraph &graph = jobGroup->GetGraph();

    ss.clear();
    ss.seekg( 0, ss.beg );
    std::string line;
    std::vector< std::string > jobFiles;
    while( std::getline( ss, line ) )
    {
        std::istringstream ss2( line );
        std::copy( std::istream_iterator<std::string>( ss2 ),
                   std::istream_iterator<std::string>(),
                   std::back_inserter( jobFiles ) );

        int v1, v2;

        std::vector< std::string >::const_iterator first = jobFiles.begin();
        std::vector< std::string >::const_iterator second = first + 1;

        v1 = jobFileToIndex[*first];

        for( ; second != jobFiles.end(); ++second )
        {
            v2 = jobFileToIndex[*second];

            add_edge( v1, v2, graph );

            v1 = v2;
            first = second;
        }

        jobFiles.clear();
    }

    // validate graph
    {
        bool has_cycle = false;
        cycle_detector vis( has_cycle );
        depth_first_search( graph, visitor( vis ) );
        if ( has_cycle )
        {
            PS_LOG( "JobManager::PrepareJobGraph: job graph has cycle" );
            return false;
        }
    }

    typedef graph_traits<JobGraph>::vertex_iterator VertexIter;
    VertexIter i, i_end;
    for( tie( i, i_end ) = vertices( graph ); i != i_end; ++i )
    {
        Job *job = jobGroup->GetIndexToJob()[ *i ];

        job->SetJobVertex( *i );
        int deps = in_degree( *i, graph );
        job->SetNumDepends( deps );
        job->SetJobGroup( jobGroup );
    }

    return true;
}

} // namespace master

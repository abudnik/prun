#define BOOST_SPIRIT_THREADSAFE

#include <boost/property_tree/json_parser.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/topological_sort.hpp>
#include <boost/graph/visitors.hpp>
#include <iterator>
#include "job_manager.h"
#include "common/log.h"
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
    std::vector< Job * > indexToJob;

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
        std::string jobDescr, line;
        while( getline( file, line ) )
            jobDescr += line;

        Job *job = CreateJob( jobDescr );
        if ( job )
        {
            jobFileToIndex[*it] = index++;
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
        succeeded = TopologicalSort( ss, jobFileToIndex, indexToJob, jobs );
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
        Job *job = *it;
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

Job *JobManager::PopJob()
{
    return jobs_.PopJob();
}

Job *JobManager::GetTopJob()
{
    return jobs_.GetTopJob();
}

void JobManager::Initialize( const std::string &exeDir, TimeoutManager *timeoutManager )
{
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

    std::string data, line;
    while( std::getline( file, line ) )
    {
        boost::trim_right( line );
        data += line + '\n';
    }

    return common::EncodeBase64( data.c_str(), data.size(), script );
}

Job *JobManager::CreateJob( boost::property_tree::ptree &ptree ) const
{
    try
    {
        std::string fileName = ptree.get<std::string>( "script" );
        if ( fileName.empty() )
        {
            PS_LOG( "JobManager::CreateJob: empty script file name" );
            return NULL;
        }

        std::string script;
        if ( !ReadScript( fileName, script ) )
            return NULL;

        std::string language = ptree.get<std::string>( "language" );
        int priority = ptree.get<int>( "priority" );
        int timeout = ptree.get<int>( "job_timeout" );
        int queueTimeout = ptree.get<int>( "queue_timeout" );
        int taskTimeout = ptree.get<int>( "task_timeout" );
        int maxFailedNodes = ptree.get<int>( "max_failed_nodes" );
        int maxCPU = ptree.get<int>( "max_cpu" );
        bool noReschedule = ptree.get<bool>( "no_reschedule" );
        bool exclusiveExec = ptree.get<bool>( "exclusive_exec" );

        if ( taskTimeout < 0 )
            taskTimeout = -1;

        Job *job = new Job( script, language,
                            priority, maxFailedNodes, maxCPU,
                            timeout, queueTimeout, taskTimeout,
                            noReschedule, exclusiveExec );
        return job;
    }
    catch( std::exception &e )
    {
        PS_LOG( "JobManager::CreateJob exception: " << e.what() );
        return NULL;
    }
}

bool JobManager::TopologicalSort( std::istringstream &ss,
                                  std::map< std::string, int > &jobFileToIndex,
                                  const std::vector< Job * > &indexToJob,
                                  std::list< Job * > &jobs ) const
{
    using namespace boost;
    typedef adjacency_list<vecS, vecS, bidirectionalS > Graph;

    typedef boost::graph_traits<Graph>::vertex_descriptor Vertex;

    typedef std::pair< int, int > Pair;
    std::vector< Pair > edges;

    // create graph
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

            edges.push_back( Pair( v1, v2 ) );

            v1 = v2;
            first = second;
        }

        jobFiles.clear();
    }

    Graph graph( edges.begin(), edges.end(), edges.size() );

    // validate graph
    {
        bool has_cycle = false;
        cycle_detector vis( has_cycle );
        depth_first_search( graph, visitor( vis ) );
        if ( has_cycle )
        {
            PS_LOG( "JobManager::TopologicalSort: job graph has cycle" );
            return false;
        }
    }

    // topological sort
    boost::property_map<Graph, vertex_index_t>::type id = get( vertex_index, graph );

    typedef std::list< Vertex > Container;
    Container c;
    topological_sort( graph, std::front_inserter( c ) );

    // calculate job rank in graph
    std::vector< int > rank( c.size(), 0 );

    Container::const_iterator i = c.begin();
    for( ; i != c.end(); ++i )
    {
        // Walk through the in_edges an calculate the maximum rank.
        if ( in_degree( *i, graph ) > 0 )
        {
            Graph::in_edge_iterator j, j_end;
            int maxRank = 0;
            // Through the order from topological sort, we are sure that every
            // time we are using here is already initialized.
            for( tie( j, j_end ) = in_edges( *i, graph ); j != j_end; ++j )
            {
                maxRank = std::max( rank[ source( *j, graph ) ], maxRank );
            }
            rank[ *i ] = maxRank + 1;
        }
    }

    // fill jobs
    jobs.clear();

    std::vector< int >::const_iterator it_rank = rank.begin();
    Container::const_iterator it = c.begin();
    for( ; it != c.end(); ++it, ++it_rank )
    {
        int index = id[ *it ];
        Job *job = indexToJob[ index ];
        job->SetRank( *it_rank );
        jobs.push_back( job );
    }
    return true;
}

} // namespace master

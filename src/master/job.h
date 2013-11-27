#ifndef __JOB_H
#define __JOB_H

#include <list>
#include <vector>
#include <boost/thread/mutex.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <stdint.h> // int64_t

namespace master {

enum JobFlag
{
    JOB_FLAG_NO_RESCHEDULE = 1,
    JOB_FLAG_EXCLUSIVE = 2
};

typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::bidirectionalS > JobGraph;

typedef boost::graph_traits<JobGraph>::vertex_descriptor JobVertex;

class Job;
class JobGroup
{
public:
    typedef typename boost::property_map< JobGraph, boost::vertex_index_t >::type PropertyMap;

public:
    void OnJobCompletion( const JobVertex &vertex );

    JobGraph &GetGraph() { return graph_; }

    std::vector< Job * > &GetIndexToJob() { return indexToJob_; }

private:
    JobGraph graph_;
    std::vector< Job * > indexToJob_;
};

class Job
{
public:
    Job( const std::string &script, const std::string &scriptLanguage,
         int priority, int maxFailedNodes, int numExec,
         int maxClusterCPU, int maxCPU,
         int timeout, int queueTimeout, int taskTimeout,
         bool exclusive, bool noReschedule )
    : script_( script ), scriptLanguage_( scriptLanguage ),
     priority_( priority ), numDepends_( 0 ), maxFailedNodes_( maxFailedNodes ),
     numExec_( numExec ), maxClusterCPU_( maxClusterCPU ), maxCPU_( maxCPU ),
     timeout_( timeout ), queueTimeout_( queueTimeout ), taskTimeout_( taskTimeout ),
     flags_( 0 ), groupId_( -1 )
    {
        if ( noReschedule )
            flags_ |= JOB_FLAG_NO_RESCHEDULE;
        if ( exclusive )
            flags_ |= JOB_FLAG_EXCLUSIVE;

        static int64_t numJobs;
        scriptLength_ = script_.size();
        id_ = numJobs++;
    }

    ~Job()
    {
        if ( jobGroup_ )
            jobGroup_->OnJobCompletion( graphVertex_ );
    }

    const std::string &GetScript() const { return script_; }
    const std::string &GetScriptLanguage() const { return scriptLanguage_; }
    unsigned int GetScriptLength() const { return scriptLength_; }

    const std::string &GetFilePath() const { return filePath_; }
    int GetPriority() const { return priority_; }
    int GetNumDepends() const { return numDepends_; }
    int GetNumPlannedExec() const { return numPlannedExec_; }
    int GetMaxFailedNodes() const { return maxFailedNodes_; }
    int GetNumExec() const { return numExec_; }
    int GetMaxClusterCPU() const { return maxClusterCPU_; }
    int GetMaxCPU() const { return maxCPU_; }
    int GetTimeout() const { return timeout_; }
    int GetQueueTimeout() const { return queueTimeout_; }
    int GetTaskTimeout() const { return taskTimeout_; }
    bool IsNoReschedule() const { return flags_ & JOB_FLAG_NO_RESCHEDULE; }
    bool IsExclusive() const { return flags_ & JOB_FLAG_EXCLUSIVE; }
    int64_t GetJobId() const { return id_; }
    int64_t GetGroupId() const { return groupId_; }

    void SetFilePath( const std::string &filePath ) { filePath_ = filePath; }
    void SetNumPlannedExec( int val ) { numPlannedExec_ = val; }
    void SetNumDepends( int val ) { numDepends_ = val; }
    void SetGroupId( int64_t val ) { groupId_ = val; }
    void SetJobVertex( const JobVertex &vertex ) { graphVertex_ = vertex; }
    void SetJobGroup( boost::shared_ptr< JobGroup > &jobGroup ) { jobGroup_ = jobGroup; }

    void AddHost( const std::string &host ) { hosts_.insert( host ); }
    bool IsHostPermitted( const std::string &host ) const;

    void AddGroup( const std::string &group ) { groups_.insert( group ); }
    bool IsGroupPermitted( const std::string &group ) const;

    template< typename T, typename U >
    void SetCallback( T *obj, void (U::*f)( const std::string &method, const boost::property_tree::ptree &params ) )
    {
        callback_ = boost::bind( f, obj->shared_from_this(), _1, _2 );
    }

    void RunCallback( const std::string &method, const boost::property_tree::ptree &params ) const
    {
        if ( callback_ )
            callback_( method, params );
    }

private:
    std::string script_;
    std::string scriptLanguage_;
    unsigned int scriptLength_;
    std::string filePath_;

    int priority_;
    int numDepends_;
    int numPlannedExec_;
    int maxFailedNodes_;
    int numExec_;
    int maxClusterCPU_;
    int maxCPU_;
    int timeout_, queueTimeout_, taskTimeout_;
    int flags_;
    int64_t id_;
    int64_t groupId_;

    std::set< std::string > hosts_;
    std::set< std::string > groups_;

    JobVertex graphVertex_;
    boost::shared_ptr< JobGroup > jobGroup_;
    boost::function< void (const std::string &method, const boost::property_tree::ptree &params) > callback_;
};

class JobQueue
{
    typedef std::map< int64_t, Job * > IdToJob;
    typedef std::list< Job * > JobList;

public:
    JobQueue() : numJobs_( 0 ) {}

    void PushJob( Job *job, int64_t groupId );
    void PushJobs( std::list< Job * > &jobs, int64_t groupId );

    Job *PopJob();
    Job *GetTopJob();

    Job *GetJobById( int64_t jobId );
    bool DeleteJob( int64_t jobId );
    bool DeleteJobGroup( int64_t groupId );

    void Clear( bool doDelete = true );

private:
    void Sort( JobList &jobs );
    void PrintJobs( const JobList &jobs ) const; // debug only

private:
    JobList jobs_;
    IdToJob idToJob_;
    unsigned int numJobs_;
    boost::mutex jobsMut_;
};

} // namespace master

#endif

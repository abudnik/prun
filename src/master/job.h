#ifndef __JOB_H
#define __JOB_H

#include <list>
#include <boost/thread/mutex.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <stdint.h> // int64_t

namespace master {

enum JobFlag
{
    JOB_FLAG_NO_RESCHEDULE = 1,
    JOB_FLAG_EXCLUSIVE_EXEC = 2
};

class Job
{
public:
    Job( const std::string &script, const std::string &scriptLanguage,
         int priority, int maxFailedNodes, int maxCPU,
         int timeout, int queueTimeout, int taskTimeout,
         bool noReschedule, bool exclusiveExec )
    : script_( script ), scriptLanguage_( scriptLanguage ),
     priority_( priority ), rank_( -1 ), maxFailedNodes_( maxFailedNodes ), maxCPU_( maxCPU ),
     timeout_( timeout ), queueTimeout_( queueTimeout ), taskTimeout_( taskTimeout ),
     flags_( 0 )
    {
        if ( noReschedule )
            flags_ |= JOB_FLAG_NO_RESCHEDULE;
        if ( exclusiveExec )
            flags_ |= JOB_FLAG_EXCLUSIVE_EXEC;

        static int64_t numJobs;
        scriptLength_ = script_.size();
        id_ = numJobs++;
    }

    const std::string &GetScript() const { return script_; }
    const std::string &GetScriptLanguage() const { return scriptLanguage_; }
    unsigned int GetScriptLength() const { return scriptLength_; }

    int GetPriority() const { return priority_; }
    int GetRank() const { return rank_; }
    int GetNumPlannedExec() const { return numPlannedExec_; }
    int GetMaxFailedNodes() const { return maxFailedNodes_; }
    int GetMaxCPU() const { return maxCPU_; }
    int GetTimeout() const { return timeout_; }
    int GetQueueTimeout() const { return queueTimeout_; }
    int GetTaskTimeout() const { return taskTimeout_; }
    bool IsNoReschedule() const { return flags_ & JOB_FLAG_NO_RESCHEDULE; }
    bool IsExclusiveAccess() const { return flags_ & JOB_FLAG_EXCLUSIVE_EXEC; }
    int64_t GetJobId() const { return id_; }

    void SetNumPlannedExec( int val ) { numPlannedExec_ = val; }
    void SetRank( int val ) { rank_ = val; }

    template< typename T >
    void SetCallback( T *obj, void (T::*f)( const std::string &result ) )
    {
        callback_ = boost::bind( f, obj->shared_from_this(), _1 );
    }

    void RunCallback( const std::string &result ) const
    {
        if ( callback_ )
            callback_( result );
    }

private:
    std::string script_;
    std::string scriptLanguage_;
    unsigned int scriptLength_;

    int priority_;
    int rank_;
    int numPlannedExec_;
    int maxFailedNodes_;
    int maxCPU_;
    int timeout_, queueTimeout_, taskTimeout_;
    int flags_;
    int64_t id_;

    boost::function< void (const std::string &) > callback_;
};

class JobQueue
{
    typedef std::map< int64_t, Job * > IdToJob;

public:
    JobQueue() : numJobs_( 0 ) {}

    void PushJob( Job *job );

    Job *PopJob();
    Job *GetTopJob();

    Job *GetJobById( int64_t jobId );
    bool DeleteJob( int64_t jobId );

    void Clear( bool doDelete = true );

private:
    std::list< Job * > jobs_;
    IdToJob idToJob_;
    unsigned int numJobs_;
    boost::mutex jobsMut_;
};

} // namespace master

#endif

#ifndef __JOB_H
#define __JOB_H

#include <list>
#include <boost/thread/mutex.hpp>
#include <stdint.h> // int64_t

namespace master {

enum JobPriority
{
	JOB_PRIORITY_HIGH, JOB_PRIORITY_LOW
};

class Job
{
public:
    Job( const char *script, const char *scriptLanguage, int numNodes,
		 int maxFailedNodes, int timeout, JobPriority priority )
    : script_( script ), scriptLanguage_( scriptLanguage ), numNodes_( numNodes ),
	 maxFailedNodes_( maxFailedNodes ), timeout_( timeout ), priority_( priority )
    {
        static int64_t numJobs;
        scriptLength_ = script_.size();
        id_ = numJobs++;
    }

	const std::string &GetScript() const { return script_; }
	const std::string &GetScriptLanguage() const { return scriptLanguage_; }
	unsigned int GetScriptLength() const { return scriptLength_; }

    int GetNumNodes() const { return numNodes_; }
    int GetMaxFailedNodes() const { return maxFailedNodes_; }
    int GetTimeout() const { return timeout_; }
    JobPriority GetPriority() const { return priority_; }
    int64_t GetJobId() const { return id_; }

private:
    std::string script_;
    std::string scriptLanguage_;
    unsigned int scriptLength_;

    int numNodes_;
	int maxFailedNodes_;
    int timeout_;
    JobPriority priority_;
    int64_t id_;
};

class JobQueue
{
    typedef std::map< int64_t, Job * > IdToJob;

public:
    JobQueue() : numJobs_( 0 ) {}

    void PushJob( Job *job );

    Job *GetJobById( int64_t jobId );

private:
    std::list< Job * > jobs_;
    IdToJob idToJob_;
    unsigned int numJobs_;
    boost::mutex jobsMut_;
};

} // namespace master

#endif

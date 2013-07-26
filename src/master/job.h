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
    Job( const char *script, const char *scriptLanguage, int maxNodes,
		 int timeout, JobPriority priority )
    : script_( script ), scriptLanguage_( scriptLanguage ), maxNodes_( maxNodes ),
	 timeout_( timeout ), priority_( priority )
    {
        static int64_t numJobs;
        scriptLength_ = script_.size();
        id_ = numJobs++;
    }

	const std::string &GetScript() const { return script_; }
	const std::string &GetScriptLanguage() const { return scriptLanguage_; }
	unsigned int GetScriptLength() const { return scriptLength_; }

    int GetMaxNodes() const { return maxNodes_; }
    int GetTimeout() const { return timeout_; }
    JobPriority GetPriority() const { return priority_; }

private:
    std::string script_;
    std::string scriptLanguage_;
    unsigned int scriptLength_;

    int maxNodes_;
    int timeout_;
    JobPriority priority_;
    int64_t id_;
};

class JobQueue
{
public:
    JobQueue() : numJobs_( 0 ) {}

    void PushJob( Job *job );
    Job *PopJob();
    Job *GetTopJob();

private:
    std::list< Job * > jobs_;
    unsigned int numJobs_;
    boost::mutex jobsMut_;
};

} // namespace master

#endif

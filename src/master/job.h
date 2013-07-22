#include <list>
#include <boost/thread/mutex.hpp>

namespace master {

class Job
{
public:
    Job( const char *script, const char *scriptLanguage, unsigned int maxNodes, int timeout )
    : script_( script ), scriptLanguage_( scriptLanguage ), maxNodes_( maxNodes ), timeout_( timeout )
    {
        scriptLength_ = script_.size();
    }

	unsigned int GetScriptLength() const { return scriptLength_; }
	const std::string &GetScriptLanguage() const { return scriptLanguage_; }
	const std::string &GetScript() const { return script_; }
    unsigned int GetMaxNodes() const { return maxNodes_; }
    int GetTimeout() const { return timeout_; }

private:
    std::string script_;
    std::string scriptLanguage_;
    unsigned int scriptLength_;
    unsigned int maxNodes_;
    int timeout_;
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
    int numJobs_;
    boost::mutex jobsMut_;
};

} // namespace master

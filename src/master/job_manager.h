#ifndef __JOB_MANAGER_H
#define __JOB_MANAGER_H

#include <boost/property_tree/ptree.hpp>
#include "job.h"


namespace master {

class JobDescriptionLanguage
{
public:
    virtual ~JobDescriptionLanguage() {}
    virtual bool ParseJob( const std::string &job_description, boost::property_tree::ptree &ptree ) = 0;
};

class JDLJason : public JobDescriptionLanguage
{
public:
    bool ParseJob( const std::string &job_description, boost::property_tree::ptree &ptree );
};

class TimeoutManager;

class JobManager
{
public:
    Job *CreateJob( const std::string &job_description ) const;
    void CreateMetaJob( const std::string &meta_description, std::list< Job * > &jobs ) const;
    void PushJob( Job *job );
    void PushJobs( std::list< Job * > &jobs );

    bool GetJobById( int64_t jobId, JobPtr &job );
    bool DeleteJob( int64_t jobId );
    bool DeleteJobGroup( int64_t groupId );
    void DeleteAllJobs();

    bool PopJob( JobPtr &job );

    const std::string &GetMasterId() const { return masterId_; }

    static JobManager &Instance()
    {
        static JobManager instance_;
        return instance_;
    }

    void Initialize( const std::string &masterId, const std::string &exeDir, TimeoutManager *timeoutManager );
    void Shutdown();

private:
    bool ReadScript( const std::string &fileName, std::string &script ) const;
    Job *CreateJob( const boost::property_tree::ptree &ptree ) const;
    void ReadHosts( Job *job, const boost::property_tree::ptree &ptree ) const;
    void ReadGroups( Job *job, const boost::property_tree::ptree &ptree ) const;

    bool PrepareJobGraph( std::istringstream &ss,
                          std::map< std::string, int > &jobFileToIndex,
                          boost::shared_ptr< JobGroup > &jobGroup ) const;

private:
    JobQueue jobs_;
    TimeoutManager *timeoutManager_;
    std::string exeDir_;
    std::string masterId_;
    static int64_t numJobGroups_;
};

} // namespace master

#endif

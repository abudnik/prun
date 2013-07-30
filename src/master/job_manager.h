#ifndef __JOB_MANAGER_H
#define __JOB_MANAGER_H

#include <boost/property_tree/ptree.hpp>
#include <set>
#include <queue>
#include "job.h"
#include "worker.h"


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

class JobManager
{
public:
    bool PushJob( const std::string &job_description );

    void SetExeDir( const std::string &dir ) { exeDir_ = dir; }

    static JobManager &Instance()
    {
        static JobManager instance_;
        return instance_;
    }

private:
    bool ReadScript( const std::string &fileName, std::string &script ) const;
    Job *CreateJob( boost::property_tree::ptree &ptree ) const;

private:
    JobQueue jobs_;
    std::string exeDir_;

	IPToWorker busyWorkers, freeWorkers;

	std::map< int64_t, std::set< std::string > > failedWorkers; // job_id -> set(worker_ip)

	std::queue< WorkerJob > needReschedule_;
};

} // namespace master

#endif

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Unit tests
#include <boost/test/unit_test.hpp>
#include <vector>
#include <list>
#include "mock.h"
#include "master/worker_manager.h"
#include "master/timeout_manager.h"
#include "master/job_manager.h"
#include "master/scheduler.h"
#include "common/service_locator.h"

using namespace std;
using namespace master;

#include "unit_worker_manager.h"
#include "unit_job_manager.h"
#include "unit_scheduler.h"
#include "unit_scheduled_jobs.h"

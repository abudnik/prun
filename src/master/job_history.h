/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2013 Andrey Budnik <budnik27@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

===========================================================================
*/

#ifndef __JOB_HISTORY_H
#define __JOB_HISTORY_H

#include "job.h"
#include "dbconnection.h"


namespace master {

struct IJobEventReceiver
{
    virtual ~IJobEventReceiver() {}
    virtual void OnJobAdd( const JobPtr &job ) = 0;
    virtual void OnJobDelete( int64_t jobId ) = 0;
};

class JobHistory: public IJobEventReceiver
{
public:
    JobHistory( IHistoryChannel *channel );

    // IJobEventReceiver
    virtual void OnJobAdd( const JobPtr &job );
    virtual void OnJobDelete( int64_t jobId );

    void OnAddCompleted( const std::string &response );
    void OnDeleteCompleted( const std::string &response );

private:
    IHistoryChannel *channel_;
    IHistoryChannel::Callback addCallback_;
    IHistoryChannel::Callback deleteCallback_;
};

} // namespace master

#endif

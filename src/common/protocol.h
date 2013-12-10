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

#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include <string>
#include <set>
#include <list>
#include <stdint.h>

namespace common {

class Protocol
{
public:
    virtual ~Protocol() {}

    // ping section
    virtual bool NodePing( std::string &msg, const std::string &hostName ) = 0;
    virtual bool NodeResponsePing( std::string &msg, int numCPU, int64_t memSizeMb ) = 0;
    virtual bool ParseResponsePing( const std::string &msg, int &numCPU, int64_t &memSizeMb ) = 0;
    virtual bool NodeJobCompletionPing( std::string &msg, int64_t jobId, int taskId ) = 0;
    virtual bool ParseJobCompletionPing( const std::string &msg, int64_t &jobId, int &taskId ) = 0;

    // script sending & results parsing
    virtual bool SendScript( std::string &msg, const std::string &scriptLanguage,
                             const std::string &script, const std::string &filePath,
                             const std::string &masterId,
                             int64_t jobId, const std::set<int> &tasks,
                             int numTasks, int timeout ) = 0;

    virtual bool ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                  std::string &script, std::string &filePath,
                                  std::string &masterId,
                                  int64_t &jobId, std::set<int> &tasks,
                                  int &numTasks, int &timeout ) = 0;

    virtual bool GetJobResult( std::string &msg, const std::string &masterId, int64_t jobId, int taskId ) = 0;
    virtual bool ParseGetJobResult( const std::string &msg, std::string &masterId, int64_t &jobId, int &taskId ) = 0;
    virtual bool SendJobResult( std::string &msg, int errCode, int64_t execTime ) = 0;
    virtual bool ParseJobResult( const std::string &msg, int &errCode, int64_t &execTime ) = 0;

    // commands section
    virtual bool SendCommand( std::string &msg, const std::string &masterId, const std::string &command,
                              const std::list< std::pair< std::string, std::string > > &params ) = 0;
    virtual bool SendCommandResult( std::string &msg, int errCode ) = 0;
    virtual bool ParseSendCommandResult( const std::string &msg, int &errCode ) = 0;

    virtual bool ParseStopTask( const std::string &msg, std::string &masterId, int64_t &jobId, int &taskId ) = 0;
    virtual bool ParseStopPreviousJobs( const std::string &msg, std::string &masterId ) = 0;

    // internals
    virtual bool ParseMsgType( const std::string &msg, std::string &type ) = 0;

    static bool ParseMsg( const std::string &msg, std::string &protocol, int &version,
                          std::string &header, std::string &body );

protected:
    void AddHeader( std::string &msg );

private:
    virtual const char *GetProtocolType() const = 0;
    virtual const char *GetProtocolVersion() const = 0;
};

class ProtocolJson : public Protocol
{
public:
    virtual bool NodePing( std::string &msg, const std::string &host );

    virtual bool NodeResponsePing( std::string &msg, int numCPU, int64_t memSizeMb );

    virtual bool ParseResponsePing( const std::string &msg, int &numCPU, int64_t &memSizeMb );

    virtual bool NodeJobCompletionPing( std::string &msg, int64_t jobId, int taskId );

    virtual bool ParseJobCompletionPing( const std::string &msg, int64_t &jobId, int &taskId );

    virtual bool SendScript( std::string &msg, const std::string &scriptLanguage,
                             const std::string &script, const std::string &filePath,
                             const std::string &masterId,
                             int64_t jobId, const std::set<int> &tasks,
                             int numTasks, int timeout );

    virtual bool ParseSendScript( const std::string &msg, std::string &scriptLanguage,
                                  std::string &script, std::string &filePath,
                                  std::string &masterId,
                                  int64_t &jobId, std::set<int> &tasks,
                                  int &numTasks, int &timeout );

    virtual bool GetJobResult( std::string &msg, const std::string &masterId, int64_t jobId, int taskId );

    virtual bool ParseGetJobResult( const std::string &msg, std::string &masterId, int64_t &jobId, int &taskId );

    virtual bool SendJobResult( std::string &msg, int errCode, int64_t execTime );

    virtual bool ParseJobResult( const std::string &msg, int &errCode, int64_t &execTime );

    virtual bool SendCommand( std::string &msg, const std::string &masterId, const std::string &command,
                              const std::list< std::pair< std::string, std::string > > &params );

    virtual bool SendCommandResult( std::string &msg, int errCode );

    virtual bool ParseSendCommandResult( const std::string &msg, int &errCode );

    virtual bool ParseStopTask( const std::string &msg, std::string &masterId, int64_t &jobId, int &taskId );

    virtual bool ParseStopPreviousJobs( const std::string &msg, std::string &masterId );

    virtual bool ParseMsgType( const std::string &msg, std::string &type );

private:
    virtual const char *GetProtocolType() const { return "json"; }
    virtual const char *GetProtocolVersion() const { return "1"; }
};

class ProtocolCreator
{
public:
    virtual Protocol *Create( const std::string &protocol, int version )
    {
        if ( protocol == "json" )
            return new ProtocolJson();
        return NULL;
    }
};

} // namespace common

#endif

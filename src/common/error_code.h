#ifndef __ERROR_CODE_H
#define __ERROR_CODE_H

#include <string>

#define NODE_FATAL -1
#define NODE_JOB_COMPLETION_NOT_FOUND -2
#define NODE_JOB_TIMEOUT -3
#define NODE_LANG_NOT_SUPPORTED -4
#define NODE_SCRIPT_EXEC_FAILED -5
#define NODE_TASK_NOT_FOUND -6
#define NODE_SCRIPT_FILE_NOT_FOUND -7
#define NODE_SCRIPT_SIZE_LIMIT -8

namespace common {

inline std::string GetErrorDescription( int err )
{
    const char *description;

    switch( err )
    {
        case NODE_FATAL:
            description = "Fatal error";
            break;
        case NODE_JOB_COMPLETION_NOT_FOUND:
            description = "Job completion not found";
            break;
        case NODE_JOB_TIMEOUT:
            description = "Job timeout";
            break;
        case NODE_LANG_NOT_SUPPORTED:
            description = "Job language is not supported";
            break;
        case NODE_SCRIPT_EXEC_FAILED:
            description = "Script execution failed";
            break;
        case NODE_TASK_NOT_FOUND:
            description = "Task not found";
            break;
        case NODE_SCRIPT_FILE_NOT_FOUND:
            description = "Script file not found";
            break;
        case NODE_SCRIPT_SIZE_LIMIT:
            description = "Script size limit exceeded";
            break;
        default:
            description = nullptr;
    }

    if ( description )
        return std::to_string( err ) + " (" + description + ")";

    return std::to_string( err );
}

} // namespace common

#endif

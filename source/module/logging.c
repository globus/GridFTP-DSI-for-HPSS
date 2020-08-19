#define _GNU_SOURCE /* See feature_test_macros(7) */
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <time.h>
#include <globus_gridftp_server.h>
#include "logging.h"

GlobusDebugDeclare(GLOBUS_GRIDFTP_SERVER_HPSS);
GlobusDebugDefine(GLOBUS_GRIDFTP_SERVER_HPSS);

static const char * TaskIDToLog = NULL;
static const char * UserToLog = NULL;

void
logging_init()
{
    /*
     * The GlobusDebug*() functions expect a numeric 'level'. For simplicity,
     * we are re using log_type_t. So make sure the levels listed here are
     * in the same order as listed in the log_type_t enum.
     */
    GlobusDebugInit(GLOBUS_GRIDFTP_SERVER_HPSS, ERROR WARN INFO DEBUG TRACE);
}

// Include 'user' as the authenticated user in all log messages.
void
logging_set_user(const char * user)
{
    UserToLog = strdup(user);
}

// Include 'taskid' in all log messages.
void
logging_set_taskid(const char * taskid)
{
    TaskIDToLog = strdup(taskid);
}

static const char *
log_type_to_string(log_type_t type)
{
    switch(type)
    {
    case LOG_TYPE_ERROR:
        return "ERROR";
    case LOG_TYPE_WARN:
        return "WARN";
    case LOG_TYPE_INFO:
        return "INFO";
    case LOG_TYPE_DEBUG:
        return "DEBUG";
    case LOG_TYPE_TRACE:
        return "TRACE";
    }
    return NULL;
}

static const char *
build_log_entry(log_type_t type, const char * message_format, va_list ap)
{
    char * message = NULL;
    int rc = vasprintf(&message, message_format, ap);
    if (rc == -1)
        return NULL;

    char * entry = NULL;
    rc = asprintf(&entry,
                  "[HPSS Connector][%s] %s%s%s%s%s%s%s\n",
                  log_type_to_string(type),
                  UserToLog   ? "User="     : "",
                  UserToLog   ? UserToLog   : "",
                  UserToLog   ? " "         : "",
                  TaskIDToLog ? "TaskID="   : "",
                  TaskIDToLog ? TaskIDToLog : "",
                  TaskIDToLog ? " "         : "",
                  message);
    free(message);
    if (rc == -1)
        return NULL;
    return entry;
}

static void
send_to_gridftp_log(globus_gfs_log_type_t type, const char * entry)
{
    globus_gfs_log_message(type, entry);
}

static void
build_timestamp(char * buffer, size_t buffer_size)
{
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);
    memset(buffer, 0, sizeof(buffer_size));
    strftime(buffer, buffer_size, "%c", &tm);
}

static void
send_to_developer_log(log_type_t type, const char * entry)
{
    char timestamp[64];
    build_timestamp(timestamp, sizeof(timestamp));
    GlobusDebugPrintf(GLOBUS_GRIDFTP_SERVER_HPSS,
                      type,
                      ("[%u] %s :: %s", getpid(), timestamp, entry));
}

void
log_message(log_type_t type, const char * format, ...)
{
    const char * static_errmsg = "COULD NOT ALLOCATE MEMORY FOR LOG MESSAGE";

    va_list ap;
    va_start (ap, format);

    const char * entry = build_log_entry(type, format, ap);
    if (entry == NULL)
    {
        type = LOG_TYPE_ERROR;
        entry = static_errmsg;
    }
    
    switch(type)
    {
    case LOG_TYPE_ERROR:
        send_to_gridftp_log(GLOBUS_GFS_LOG_ERR, entry);
        send_to_developer_log(type, entry);
        break;
    case LOG_TYPE_WARN:
        send_to_gridftp_log(GLOBUS_GFS_LOG_WARN, entry);
        send_to_developer_log(type, entry);
        break;
    case LOG_TYPE_INFO:
        send_to_gridftp_log(GLOBUS_GFS_LOG_INFO, entry);
        send_to_developer_log(type, entry);
        break;
    case LOG_TYPE_DEBUG:
    case LOG_TYPE_TRACE:
        send_to_developer_log(type, entry);
        break;
    }

    if (entry != static_errmsg)
        free((char *)entry);
    va_end(ap);
}

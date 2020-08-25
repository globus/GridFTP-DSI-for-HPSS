#define _GNU_SOURCE /* See feature_test_macros(7) */
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <time.h>
#include <globus_gridftp_server.h>
#include "logging.h"
#include "strings.h"

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

static void
_log_message(log_type_t type, const char * format, va_list ap)
{
    const char * static_errmsg = "COULD NOT ALLOCATE MEMORY FOR LOG MESSAGE";

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
}

void
log_message(log_type_t type, const char * format, ...)
{
    va_list ap;
    va_start (ap, format);
    _log_message(type, format, ap);
    va_end(ap);
}

void
log_api_enter(const char * func, const char * format, ...)
{
    char * message = NULL;
    asprintf(&message, "%s() Enter: %s", func, format);

    va_list ap;
    va_start(ap, format);
    _log_message(LOG_TYPE_TRACE, message, ap);
    va_end(ap);
    free(message);
}

void
log_api_exit(const char * func, const char * format, ...)
{
    char * message = NULL;
    asprintf(&message, "%s() Exit: %s", func, format);

    va_list ap;
    va_start(ap, format);
    _log_message(LOG_TYPE_TRACE, message, ap);
    va_end(ap);
    free(message);
}

char *
_char_ptr(struct pool * pool, const char * c_ptr)
{
    if (c_ptr == NULL)
        return PTR(c_ptr);
    return _sprintf(pool, "\"%s\"", c_ptr);
}

char *
_hex(struct pool * pool, unsigned u)
{
    return _sprintf(pool, "0x%X", u);
}

char *
_hex8(struct pool * pool, unsigned char c)
{
    return _sprintf(pool, "0x%X", (unsigned)c);
}

char *
_hex64(struct pool * pool, unsigned long u)
{
    return _sprintf(pool, "0x%lX", u);
}

char *
_int(struct pool * pool, int i)
{
    return INT_PTR(&i);
}

char *
_int_ptr(struct pool * pool, const int * p)
{
    if (p == NULL)
        return PTR(p);
    return _sprintf(pool, "%d", *p);
}

char *
_ptr(struct pool * pool, const void * p)
{
   if (p == NULL)
       return "null";
   return _sprintf(pool, "%s<ptr>", HEX64((unsigned long)p));
}

char *
_struct_utimbuf_ptr(struct pool * pool, const struct utimbuf * p)
{
    if (p == NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "actime=%s, " // time_t
            "modtime=%s"  // time_t
        "}",
            TIME_T(p->actime),
            TIME_T(p->modtime));
}

char *
_unsigned(struct pool * pool, unsigned u)
{
    return _sprintf(pool, "%u", u);
}

char *
_unsigned_ptr(struct pool * pool, const unsigned * p)
{
    if (p == NULL)
        return PTR(p);
    return _sprintf(pool, "%u", *p);
}

// TODO: duplicate code of _pv_list_val
char *
_unsigned_array(struct pool * pool, const unsigned * p, size_t c)
{
    if (p == NULL)
        return PTR(p);

    char * str = "[";
    for (int i = 0; i < c; i++)
    {
        str = _sprintf(
            pool,
            "%s%s%s",
            i == 0 ? "" : str, // prefix
            i == 0 ? "" : ", ",
            UNSIGNED(p[i]));
    }
    return _strcat(pool, str, "]");
}

char *
_unsigned8(struct pool * pool, unsigned char u)
{
    return _sprintf(pool, "%u", (unsigned)u);
}

char *
_unsigned16(struct pool * pool, unsigned short s)
{
    return _sprintf(pool, "%hu", s);
}

char *
_unsigned64(struct pool * pool, unsigned long u)
{
    return _sprintf(pool, "%lu", u);
}

char *
_unsigned64_ptr(struct pool * pool, const unsigned long * p)
{
    if (p == NULL)
        return PTR(p);
    return UNSIGNED64(*p);
}

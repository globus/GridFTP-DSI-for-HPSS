#ifndef _LOGGING_H_
#define _LOGGING_H_

#include <stdarg.h>
#include <globus_common.h>

#define ERROR(...) log_message(LOG_TYPE_ERROR, __VA_ARGS__)

#define WARN(...) log_message(LOG_TYPE_WARN, __VA_ARGS__)

#define INFO(...) log_message(LOG_TYPE_INFO, __VA_ARGS__)

#define DEBUG(...) log_message(LOG_TYPE_DEBUG, __VA_ARGS__)

#define TRACE(...) log_message(LOG_TYPE_TRACE, __VA_ARGS__)

void
logging_init();

typedef enum {
    LOG_TYPE_ERROR = 1<<0,
    LOG_TYPE_WARN  = 1<<1,
    LOG_TYPE_INFO  = 1<<2,
    LOG_TYPE_DEBUG = 1<<3,
    LOG_TYPE_TRACE = 1<<4
} log_type_t;

void
log_message(log_type_t type, const char * format, ...);

#endif /* _LOGGING_H_ */

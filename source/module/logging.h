#ifndef _LOGGING_H_
#define _LOGGING_H_

#include <sys/types.h>
#include <stdarg.h>
#include <utime.h>
#include <globus_common.h>
#include "pool.h"

#define ERROR(...) log_message(LOG_TYPE_ERROR, __VA_ARGS__)

#define WARN(...) log_message(LOG_TYPE_WARN, __VA_ARGS__)

#define INFO(...) log_message(LOG_TYPE_INFO, __VA_ARGS__)

#define DEBUG(...) log_message(LOG_TYPE_DEBUG, __VA_ARGS__)

#define TRACE(...) log_message(LOG_TYPE_TRACE, __VA_ARGS__)

#define API_ENTER(func, format, ... /* format, args */) \
{                                                       \
    struct pool * pool = &(struct pool){NULL, 0, 0};    \
    pool_create(pool);                                  \
    log_api_enter(func, format, ##__VA_ARGS__);         \
    pool_destroy(pool);                                 \
}
void
log_api_enter(const char * func, const char * format, ...);

#define API_EXIT(func, format, ... /* format, args */) \
{                                                      \
    struct pool * pool = &(struct pool){NULL, 0, 0};   \
    pool_create(pool);                                 \
    log_api_exit(func, format, ##__VA_ARGS__);         \
    pool_destroy(pool);                                \
}
void
log_api_exit(const char * func, const char * format, ...);

#define CHAR_PTR(c_ptr) _char_ptr(pool, c_ptr)
char *
_char_ptr(struct pool * pool, const char * c_ptr);

#define HEX(u) _hex(pool, u)
char *
_hex(struct pool * pool, unsigned u);

#define HEX8(b) _hex8(pool, b)
char *
_hex8(struct pool * pool, unsigned char b);

#define HEX64(b) _hex64(pool, b)
char *
_hex64(struct pool * pool, unsigned long b);

#define INT(i) _int(pool, i)
char *
_int(struct pool * pool, int i);

#define INT_PTR(p) _int_ptr(pool, p)
char *
_int_ptr(struct pool * pool, const int * p);

#define MODE_T(m) HEX(m)

#define PTR(p) _ptr(pool, p)
char *
_ptr(struct pool * pool, const void * p);

#define SIGNED(s) INT(s)

#define SIGNED_PTR(s) INT_PTR(s)

#define STRUCT_UTIMBUF_PTR(p) _struct_utimbuf_ptr(pool, p)
char *
_struct_utimbuf_ptr(struct pool * pool, const struct utimbuf * p);

#define TIME_T(t) UNSIGNED(t)

#define UID_T(u) UNSIGNED(u)

#define UNSIGNED(i) _unsigned(pool, i)
char *
_unsigned(struct pool * pool, unsigned u);

#define UNSIGNED_PTR(p) _unsigned_ptr(pool, p)
char *
_unsigned_ptr(struct pool * pool, const unsigned * p);

#define UNSIGNED_ARRAY(p, c) _unsigned_array(pool, p, c)
char *
_unsigned_array(struct pool * pool, const unsigned * p, size_t c);

#define UNSIGNED8(u) _unsigned8(pool, u)
char *
_unsigned8(struct pool * pool, unsigned char u);

#define UNSIGNED16(i) _unsigned16(pool, i)
char *
_unsigned16(struct pool * pool, unsigned short s);

#define UNSIGNED64(i) _unsigned64(pool, i)
char *
_unsigned64(struct pool * pool, unsigned long u);

#define UNSIGNED64_PTR(p) _unsigned64_ptr(pool, p)
char *
_unsigned64_ptr(struct pool * pool, const unsigned long * p);

void
logging_init();

// Include 'user' as the authenticated user in all log messages.
void
logging_set_user(const char * user);

// Include 'taskid' in all log messages.
void
logging_set_taskid(const char * taskid);

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

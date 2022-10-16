/*
 * System includes
 */
#define _GNU_SOURCE         /* See feature_test_macros(7) */
#include <string.h>
#include <stdio.h>

/*
 * Local includes
 */
#include "strings.h"

char *
_sprintf(struct pool * pool, const char * format, ...)
{
    va_list ap;
    va_start(ap, format);
    int rc = vsnprintf(NULL, 0, format, ap);
    va_end(ap);

    char * str = pool_alloc(pool, rc + 1);

    va_start(ap, format);
    vsnprintf(str, rc + 1, format, ap);
    va_end(ap);

    return str;
}

char *
_strcat(struct pool * pool, const char * prefix, const char * suffix)
{
    return _sprintf(pool, "%s%s", prefix ? prefix : "", suffix);
}

char *
_strdup(struct pool * pool, const char * str)
{
    return _sprintf(pool, "%s", str);
}

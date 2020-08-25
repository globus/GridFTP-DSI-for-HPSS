#ifndef _STRINGS_H_
#define _STRINGS_H_

#include <stdarg.h>
#include "pool.h"

char *
_sprintf(struct pool * pool, const char * format, ...);

char *
_strcat(struct pool * pool, const char * prefix, const char * suffix);

char *
_strdup(struct pool * pool, const char * str);

#endif /* _STRINGS_H_ */

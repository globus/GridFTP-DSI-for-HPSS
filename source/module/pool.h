#ifndef _POOL_H_
#define _POOL_H_

/*
 * System includes
 */
#include <stddef.h>

/*
 * A pool is just a record of memory allocations that can be easily
 * released with a call to pool_destroy(). This is mainly for making
 * logging calls cleaner.
 */

struct pool {
    void ** mem;
    int     off;
    int     cnt;
};

void
pool_create(struct pool * pool);

void *
pool_alloc(struct pool * pool, size_t len);

void
pool_destroy(struct pool * pool);

#endif /* _POOL_H_ */

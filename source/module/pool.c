/*
 * System includes.
 */
#include <stdlib.h>
#include <string.h>

/*
 * Local includes.
 */
#include "pool.h"

void
pool_create(struct pool * pool)
{
    memset(pool, 0, sizeof(*pool));
}

void *
pool_alloc(struct pool * pool, size_t len)
{
    if (pool->off == pool->cnt)
    {
        pool->cnt += 128;
        pool->mem = realloc(pool->mem, pool->cnt*sizeof(void *));
    }
    pool->mem[pool->off] = calloc(1, len);
    return pool->mem[pool->off++];
}

void
pool_destroy(struct pool * pool)
{
    for (int i = 0; i < pool->off; i++)
    {
        free(pool->mem[i]);
    }
    free(pool->mem);
}

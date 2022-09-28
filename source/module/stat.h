#ifndef HPSS_DSI_STAT_H
#define HPSS_DSI_STAT_H

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

globus_result_t
stat_object(char *Pathname, globus_gfs_stat_t *);

globus_result_t
stat_link(char *Pathname, globus_gfs_stat_t *);

typedef globus_result_t (*stat_dir_cb)(globus_gfs_stat_t * Array,
                                       uint32_t            ArrayLength,
                                       uint32_t            End,
                                       void              * CallbackArg);

globus_result_t
stat_directory(char      * Pathname,
               stat_dir_cb Callback,
               void      * CallbackArg);

void
stat_destroy(globus_gfs_stat_t *);

void
stat_destroy_array(globus_gfs_stat_t *, int Count);

#endif /* HPSS_DSI_STAT_H */

#ifndef _FIXUPS_H_
#define _FIXUPS_H_

/*
 * System includes
 */
#include <stdint.h>

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

globus_result_t
fixup_stat_object(char              * Pathname, 
                  globus_result_t     Result, 
                  globus_gfs_stat_t * GFSstat);

globus_result_t
fixup_stat_directory(char              * Pathname,
                     globus_gfs_stat_t * GFSstat,
                     uint32_t          * CountOut);

globus_result_t
fixup_rmd(char * Pathname, globus_result_t Result);

#endif /* _FIXUPS_H_ */

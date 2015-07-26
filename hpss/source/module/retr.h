/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2015 NCSA.  All rights reserved.
 *
 * Developed by:
 *
 * Storage Enabling Technologies (SET)
 *
 * Nation Center for Supercomputing Applications (NCSA)
 *
 * http://www.ncsa.illinois.edu
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the .Software.),
 * to deal with the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 *    + Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimers.
 *
 *    + Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimers in the
 *      documentation and/or other materials provided with the distribution.
 *
 *    + Neither the names of SET, NCSA
 *      nor the names of its contributors may be used to endorse or promote
 *      products derived from this Software without specific prior written
 *      permission.
 *
 * THE SOFTWARE IS PROVIDED .AS IS., WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS WITH THE SOFTWARE.
 */
#ifndef HPSS_DSI_RETR_H
#define HPSS_DSI_RETR_H

/*
 * System includes
 */
#include <pthread.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>
#include <globus_list.h>

/*
 * Local includes
 */
#include "pio.h"

typedef struct {
	globus_gfs_operation_t       Operation;
	globus_gfs_transfer_info_t * TransferInfo;

	int FileFD;

	uint64_t Offset; // Just for sanity checking

	globus_result_t Result;
	globus_size_t   BlockSize;

	pthread_mutex_t Mutex;
	pthread_cond_t  Cond;

	int OptConnCnt;
	int ConnChkCnt;

	globus_list_t * AllBufferList;
	globus_list_t * FreeBufferList;

} retr_info_t;

void
retr(globus_gfs_operation_t       Operation,
     globus_gfs_transfer_info_t * TransferInfo);

#endif /* HPSS_DSI_RETR_H */

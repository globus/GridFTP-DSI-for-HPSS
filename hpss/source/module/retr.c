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

/*
 * System includes
 */
#include <assert.h>

/*
 * Local includes
 */
#include "retr.h"
#include "pio.h"

int
retr_pio_callout(char     * Buffer,
                 uint32_t * Length,
                 uint64_t   Offset,
                 void     * CallbackArg);

void
retr_pio_completion_callback(globus_result_t Result,
                             void          * UserArg);

globus_result_t
retr_open_for_reading(char * Pathname,
	                  int  * FileFD,
	                  int  * FileStripeWidth)
{
	hpss_cos_hints_t      hints_in;
	hpss_cos_hints_t      hints_out;
	hpss_cos_priorities_t priorities;

	GlobusGFSName(retr_open_for_reading);

	*FileFD = -1;

	/* Initialize the hints in. */
	memset(&hints_in, 0, sizeof(hpss_cos_hints_t));

	/* Initialize the hints out. */
	memset(&hints_out, 0, sizeof(hpss_cos_hints_t));

	/* Initialize the priorities. */
	memset(&priorities, 0, sizeof(hpss_cos_priorities_t));

	/* Open the HPSS file. */
	*FileFD = hpss_Open(Pathname,
                    	O_RDONLY,
                    	S_IRUSR|S_IWUSR,
                    	&hints_in,
                    	&priorities,
                    	&hints_out);
	if (*FileFD < 0)
		return GlobusGFSErrorSystemError("hpss_Open", -(*FileFD));

	/* Copy out the file stripe width. */
	*FileStripeWidth = hints_out.StripeWidth;

    return GLOBUS_SUCCESS;
}

void
retr(globus_gfs_operation_t       Operation,
     globus_gfs_transfer_info_t * TransferInfo)
{
	int             rc                = 0;
	retr_info_t   * retr_info         = NULL;
	globus_result_t result            = GLOBUS_SUCCESS;
	int             file_stripe_width = 0;
	hpss_stat_t     hpss_stat_buf;

	GlobusGFSName(retr);

	/*
	 * Create our structure.
	 */
	retr_info = malloc(sizeof(retr_info_t));
	if (!retr_info)
	{
		result = GlobusGFSErrorMemory("retr_info_t");
		goto cleanup;
	}
	memset(retr_info, 0, sizeof(retr_info_t));
	retr_info->Operation    = Operation;
	retr_info->TransferInfo = TransferInfo;
	retr_info->FileFD       = -1;
	pthread_mutex_init(&retr_info->Mutex, NULL);
	pthread_cond_init(&retr_info->Cond, NULL);

	globus_gridftp_server_get_block_size(Operation, &retr_info->BlockSize);

	rc = hpss_Stat(TransferInfo->pathname, &hpss_stat_buf);
	if (rc)
	{
		result = GlobusGFSErrorSystemError("hpss_Stat", -rc);
		goto cleanup;
	}

	/*
	 * Open the file.
	 */
	result = retr_open_for_reading(TransferInfo->pathname,
	                               &retr_info->FileFD,
	                               &file_stripe_width);
	if (result) goto cleanup;

	/*
	 * Setup PIO
	 */
	result = pio_start(HPSS_PIO_READ,
	                   retr_info->FileFD,
	                   file_stripe_width,
	                   retr_info->BlockSize,
	                   hpss_stat_buf.st_size,
	                   retr_pio_callout,
	                   retr_pio_completion_callback,
	                   retr_info);

cleanup:
	if (result)
	{
		globus_gridftp_server_finished_transfer(Operation, result);
		if (retr_info)
		{
			if (retr_info->FileFD != -1)
				hpss_Close(retr_info->FileFD);
			pthread_mutex_destroy(&retr_info->Mutex);
			pthread_cond_destroy(&retr_info->Cond);
			free(retr_info);
		}
	}
}

void
retr_gridftp_callout(globus_gfs_operation_t Operation,
                     globus_result_t        Result,
                     globus_byte_t        * Buffer,
                     globus_size_t          Length,
                     void                 * UserArg)
{
	retr_info_t * retr_info = UserArg;

	pthread_mutex_lock(&retr_info->Mutex);
	{
		if (Result && !retr_info->Result) retr_info->Result = Result;
assert(Length  <= retr_info->BlockSize);
		pthread_cond_signal(&retr_info->Cond);
	}
	pthread_mutex_unlock(&retr_info->Mutex);
}

int
retr_pio_callout(char     * Buffer,
                 uint32_t * Length,
                 uint64_t   Offset,
                 void     * CallbackArg)
{
	int             rc        = 0;
	retr_info_t   * retr_info = CallbackArg;
	globus_result_t result    = GLOBUS_SUCCESS;

	GlobusGFSName(retr_pio_callout);

	pthread_mutex_lock(&retr_info->Mutex);
	{
assert(Offset == retr_info->Offset);
assert(*Length <= retr_info->BlockSize);

		if (!retr_info->Started)
			globus_gridftp_server_begin_transfer(retr_info->Operation, 0, NULL);
		retr_info->Started = 1;

		result = globus_gridftp_server_register_write(retr_info->Operation,
		                                              (globus_byte_t *)Buffer,
		                                              *Length,
		                                              Offset,
		                                              -1,
		                                              retr_gridftp_callout,
		                                              retr_info);

		if (result)
		{
			rc = 1; /* Signal to shutdown. */
			if (!retr_info->Result) retr_info->Result = result;
			goto cleanup;
		}

		pthread_cond_wait(&retr_info->Cond, &retr_info->Mutex);

		if (retr_info->Result)
		{
			rc = 1; /* Signal to shutdown. */
			goto cleanup;
		}

		retr_info->Offset += *Length;
	}
cleanup:
	pthread_mutex_unlock(&retr_info->Mutex);

	return rc;
}

void
retr_pio_completion_callback (globus_result_t Result,
                              void          * UserArg)
{
	globus_result_t result    = Result;
	retr_info_t   * retr_info = UserArg;
	int             rc        = 0;

	GlobusGFSName(retr_pio_completion_callback);

	/* Give our error priority. */
	if (retr_info->Result)
		result = retr_info->Result;

	rc = hpss_Close(retr_info->FileFD);
	if (rc && !result)
		result = GlobusGFSErrorSystemError("hpss_Close", -rc);

	globus_gridftp_server_finished_transfer(retr_info->Operation, result);

	pthread_mutex_destroy(&retr_info->Mutex);
	pthread_cond_destroy(&retr_info->Cond);
	free(retr_info);
}


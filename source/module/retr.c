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
#include "markers.h"
#include "retr.h"
#include "pio.h"

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
retr_gridftp_callout(globus_gfs_operation_t Operation,
                     globus_result_t        Result,
                     globus_byte_t        * Buffer,
                     globus_size_t          Length,
                     void                 * UserArg)
{
	retr_buffer_t * retr_buffer = UserArg;
	retr_info_t   * retr_info   = retr_buffer->RetrInfo;

	if (retr_buffer->Valid != VALID_TAG) return;

	pthread_mutex_lock(&retr_info->Mutex);
	{
		if (Result && !retr_info->Result) retr_info->Result = Result;

		globus_list_insert(&retr_info->FreeBufferList, retr_buffer);
assert(Length  <= retr_info->BlockSize);
		pthread_cond_signal(&retr_info->Cond);
	}
	pthread_mutex_unlock(&retr_info->Mutex);
}

/*
 * Called locked.
 */
globus_result_t
retr_get_free_buffer(retr_info_t   *  RetrInfo,
                     retr_buffer_t ** FreeBuffer)
{
	int all_buf_cnt  = 0;
	int free_buf_cnt = 0;
	int cur_conn_cnt = 0;

	GlobusGFSName(retr_get_free_buffer);

	/*
	 * Wait for a free buffer or wait until conditions are right to create one.
	 */
	while (1)
	{
		/*
		 * Check for the optimal number of concurrent writes.
		 */
		if (RetrInfo->ConnChkCnt++ == 0)
			globus_gridftp_server_get_optimal_concurrency(RetrInfo->Operation,
		                                             &RetrInfo->OptConnCnt);
		if (RetrInfo->ConnChkCnt >= 100)
			RetrInfo->ConnChkCnt = 0;

		/* Check for error first. */
		if (RetrInfo->Result)
			return RetrInfo->Result;

		/* We can exit the loop if we have less than OptConnCnt buffers in use. */
		all_buf_cnt  = globus_list_size(RetrInfo->AllBufferList);
		free_buf_cnt = globus_list_size(RetrInfo->FreeBufferList);

		cur_conn_cnt = all_buf_cnt - free_buf_cnt;
		if (cur_conn_cnt < RetrInfo->OptConnCnt)
			break;

		pthread_cond_wait(&RetrInfo->Cond, &RetrInfo->Mutex);
	}

	if (!globus_list_empty(RetrInfo->FreeBufferList))
	{
		*FreeBuffer = globus_list_remove(&RetrInfo->FreeBufferList, RetrInfo->FreeBufferList);
		return GLOBUS_SUCCESS;
	}

	*FreeBuffer = malloc(sizeof(retr_buffer_t));
	if (!*FreeBuffer)
		return GlobusGFSErrorMemory("free_buffer");
	(*FreeBuffer)->Buffer = malloc(RetrInfo->BlockSize);
	if (!(*FreeBuffer)->Buffer)
		return GlobusGFSErrorMemory("free_buffer");
	(*FreeBuffer)->RetrInfo = RetrInfo;
	(*FreeBuffer)->Valid    = VALID_TAG;
	globus_list_insert(&RetrInfo->AllBufferList, *FreeBuffer);
	return GLOBUS_SUCCESS;
}

int
retr_pio_callout(char     * ReadyBuffer,
                 uint32_t * Length,
                 uint64_t   Offset,
                 void     * CallbackArg)
{
	int             rc           = 0;
	retr_buffer_t * free_buffer  = NULL;
	retr_info_t   * retr_info    = CallbackArg;
	globus_result_t result       = GLOBUS_SUCCESS;

	GlobusGFSName(retr_pio_callout);

	assert (Offset == retr_info->CurrentOffset);

	pthread_mutex_lock(&retr_info->Mutex);
	{
assert(*Length <= retr_info->BlockSize);

		result = retr_get_free_buffer(retr_info, &free_buffer);
		if (result)
		{
			if (!retr_info->Result) retr_info->Result = result;
			rc = PIO_END_TRANSFER; /* Signal to shutdown. */
			goto cleanup;
		}

		memcpy(free_buffer->Buffer, ReadyBuffer, *Length);

		result = globus_gridftp_server_register_write(retr_info->Operation,
		                                              (globus_byte_t *)free_buffer->Buffer,
		                                              *Length,
		                                              Offset,
		                                              -1,
		                                              retr_gridftp_callout,
		                                              free_buffer);

		if (result)
		{
			if (!retr_info->Result) retr_info->Result = result;
			rc = PIO_END_TRANSFER; /* Signal to shutdown. */
			goto cleanup;
		}

		/* Update perf markers */
		markers_update_perf_markers(retr_info->Operation, Offset, *Length);
	}
cleanup:
	pthread_mutex_unlock(&retr_info->Mutex);

	retr_info->CurrentOffset += *Length;
	return rc;
}

void
retr_wait_for_gridftp(retr_info_t * RetrInfo)
{
	pthread_mutex_lock(&RetrInfo->Mutex);
	{
		while (1)
		{
			if (RetrInfo->Result) break;

			if (globus_list_size(RetrInfo->AllBufferList) == globus_list_size(RetrInfo->FreeBufferList))
				break;

			pthread_cond_wait(&RetrInfo->Cond, &RetrInfo->Mutex);
		}
	}
	pthread_mutex_unlock(&RetrInfo->Mutex);
}

void
retr_range_complete_callback(globus_off_t * Offset,
                             globus_off_t * Length,
                             int          * Eot,
                             void         * UserArg)
{
	retr_info_t * retr_info = UserArg;

	assert(*Length <= retr_info->RangeLength);

	// PIO is telling us that this range (Offset, Length) is complete. However,
	// it is possible that we did not actually transfer this entire length because
	// PIO has come across a hole in the file. 
	char * buffer = calloc(1, retr_info->BlockSize);
	while (retr_info->CurrentOffset < (*Length + *Offset))
	{
		globus_off_t fill_size = retr_info->CurrentOffset - (*Length + *Offset);
		if (fill_size > retr_info->BlockSize)
			fill_size = retr_info->BlockSize;

		// Send it
		retr_pio_callout(buffer, &fill_size, retr_info->CurrentOffset, retr_info);
	}
	free(buffer);

	retr_info->RangeLength -= *Length;
	*Offset                += *Length;
	*Length                 = retr_info->RangeLength;

	*Eot = 0;
	if (retr_info->RangeLength == 0)
	{
		globus_gridftp_server_get_read_range(retr_info->Operation, Offset, Length);
		if (*Length == 0)
			*Eot = 1;
		if (*Length == -1)
			*Length = retr_info->FileSize - *Offset;
		retr_info->RangeLength   = *Length;
		retr_info->CurrentOffset = *Offset;
	}
}

static int
release_buffer(void * Datum, void * Arg)
{
	((retr_buffer_t *)Datum)->Valid = INVALID_TAG;
	free(((retr_buffer_t *)Datum)->Buffer);

	return 0;
}

void
retr_transfer_complete_callback (globus_result_t Result,
                                 void          * UserArg)
{
	globus_result_t result    = Result;
	retr_info_t   * retr_info = UserArg;
	int             rc        = 0;

	GlobusGFSName(retr_transfer_complete_callback);

	globus_gridftp_server_finished_transfer(retr_info->Operation, result);
	retr_wait_for_gridftp(retr_info);

	/* Prefer our error over PIO's */
	if (retr_info->Result)
		result = retr_info->Result;

	if (result)
		result = retr_info->Result;

	rc = hpss_Close(retr_info->FileFD);
	if (rc && !result)
		result = GlobusGFSErrorSystemError("hpss_Close", -rc);


	pthread_mutex_destroy(&retr_info->Mutex);
	pthread_cond_destroy(&retr_info->Cond);
	globus_list_free(retr_info->FreeBufferList);
	globus_list_search_pred(retr_info->AllBufferList, release_buffer, NULL);
	globus_list_destroy_all(retr_info->AllBufferList, free);
	free(retr_info);
}

void
retr(globus_gfs_operation_t       Operation,
     globus_gfs_transfer_info_t * TransferInfo)
{
	int             rc                = 0;
	int             file_stripe_width = 0;
	retr_info_t   * retr_info         = NULL;
	globus_result_t result            = GLOBUS_SUCCESS;
	hpss_stat_t     hpss_stat_buf;

	GlobusGFSName(retr);

	rc = hpss_Stat(TransferInfo->pathname, &hpss_stat_buf);
	if (rc)
	{
		result = GlobusGFSErrorSystemError("hpss_Stat", -rc);
		goto cleanup;
	}

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
	retr_info->FileSize     = hpss_stat_buf.st_size;
	pthread_mutex_init(&retr_info->Mutex, NULL);
	pthread_cond_init(&retr_info->Cond, NULL);

	globus_gridftp_server_get_block_size(Operation, &retr_info->BlockSize);

	/*
	 * Open the file.
	 */
	result = retr_open_for_reading(TransferInfo->pathname,
	                               &retr_info->FileFD,
	                               &file_stripe_width);
	if (result) goto cleanup;

	globus_gridftp_server_begin_transfer(Operation, 0, NULL);

	globus_gridftp_server_get_read_range(Operation,
	                                     &retr_info->CurrentOffset, 
	                                     &retr_info->RangeLength);
	if (retr_info->RangeLength == -1)
		retr_info->RangeLength = retr_info->FileSize - retr_info->CurrentOffset;

	/*
	 * Setup PIO
	 */
	result = pio_start(HPSS_PIO_READ,
	                   retr_info->FileFD,
	                   file_stripe_width,
	                   retr_info->BlockSize,
	                   retr_info->CurrentOffset,
	                   retr_info->RangeLength,
	                   retr_pio_callout,
	                   retr_range_complete_callback,
	                   retr_transfer_complete_callback,
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


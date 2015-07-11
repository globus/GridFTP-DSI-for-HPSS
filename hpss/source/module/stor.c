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
 * Local includes
 */
#include "stor.h"
#include "pio.h"

void
stor_pio_callback(globus_result_t   Result,
                  char            * Buffer,
                  uint32_t          Length,
                  void            * CallbackArg);

void
stor_gridfp_callback(globus_gfs_operation_t  Operation,
                     globus_result_t         Result,
                     globus_byte_t         * Buffer,
                     globus_size_t           Length,
                     globus_off_t            Offset,
                     globus_bool_t           Eof,
                     void                  * CallbackArg);

globus_result_t
stor_register_gridftp_reads(stor_info_t  * StorInfo,
                            char         * Buffer,
                            globus_size_t  Length)
{
	char          * buffer = Buffer;
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(stor_register_gridftp_reads);

	pthread_mutex_lock(&StorInfo->Mutex);
	{
		/*
		 * Check optimal concurrency.
		 */
		if (StorInfo->OptConChkCnt == 0)
		{
			globus_gridftp_server_get_optimal_concurrency(StorInfo->Operation,
			                                              &StorInfo->OptConCnt);
		}
		if (StorInfo->OptConChkCnt++ >= 100)
			StorInfo->OptConChkCnt = 0;

		while (buffer || StorInfo->OptConCnt > globus_list_size(StorInfo->BufferList))
		{
			/*
			 * Allocate a buffer if we weren't given one.
			 */
			if (!buffer)
			{
				buffer = globus_malloc(StorInfo->BlockSize);
				if (!buffer)
				{
					result = GlobusGFSErrorMemory("buffer");
					goto cleanup;
				}

				globus_list_insert(&StorInfo->BufferList, buffer);
			}

			/*
			 * Request a new read.
			 */
			result = globus_gridftp_server_register_read(StorInfo->Operation,
			                                             (globus_byte_t*)buffer,
			                                             StorInfo->BlockSize,
			                                             stor_gridfp_callback,
			                                             StorInfo);
			if (result != GLOBUS_SUCCESS)
				goto cleanup;

			/* Release our reference to buffer. */
			buffer = NULL;
		}
	}
cleanup:
	pthread_mutex_unlock(&StorInfo->Mutex);

	return result;
}

/*
 * We need to populate RangeList with the File Offsets to transfer.
 * TransferInfo->partial_length will be  = -1.
 * TransferInfo->partial_offset will be >= 0
 * TransferInfo->alloc_size = the amount of incoming data
 *
 * RangeList is the inverse of the restart markers; it is the transfer
 * offsets that should be transferred. The last entry will have a length
 * of -1 meaning end of file.
 */
globus_result_t
stor_fill_range_list(globus_range_list_t          RangeList,
                     globus_gfs_transfer_info_t * TransferInfo)
{
	int             index                 = 0;
	globus_off_t    range_offset          = 0;
	globus_off_t    range_length          = 0;
	globus_off_t    remaining_file_length = 0;
	globus_off_t    starting_file_offset  = 0;
	globus_result_t result                = GLOBUS_SUCCESS;

	GlobusGFSName(stor_file_range_list);

	/* Get the starting file offset. */
	starting_file_offset = TransferInfo->partial_offset;

	/* Get the remaining file length. */
	remaining_file_length = TransferInfo->alloc_size;

	/* For each range in the restart list. */
	for (index = 0;
	     index < globus_range_list_size(TransferInfo->range_list);
	     index++)
	{
		globus_range_list_at(TransferInfo->range_list, index, &range_offset, &range_length);

		/* Convert from transfer offset to file offset. */
		range_offset += TransferInfo->partial_offset;

		/* Skip zero-length ranges. */
		if (range_length == 0)
			continue;

		/* -1 is code for 'remainder of transfer'. */
		if (range_length == -1)
			range_length = remaining_file_length;

		/* Truncate the range to fit the alloc_size. */
		if (range_length > remaining_file_length)
			range_length = remaining_file_length;

		/* Insert this range into the list. */
		result = globus_range_list_insert(RangeList, range_offset, range_length);
		if (result != GLOBUS_SUCCESS)
			return result;

		/* Decrement the remaining file length to transfer. */
		remaining_file_length -= range_length;

		/* Bail if there is nothing to transfer. */
		if (remaining_file_length == 0)
			break;
	}

    return GLOBUS_SUCCESS;
}

void
stor(globus_gfs_operation_t       Operation,
     globus_gfs_transfer_info_t * TransferInfo)
{
	globus_result_t result    = GLOBUS_SUCCESS;
	stor_info_t   * stor_info = NULL;

	GlobusGFSName(stor);

	/*
	 * Allocate our stor_info_t struct.
	 */
	stor_info = globus_malloc(sizeof(stor_info_t));
	if (!stor_info)
	{
		result = GlobusGFSErrorMemory("stor_info_t");
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}
	memset(stor_info, 0, sizeof(*stor_info));

	pthread_mutexattr_t mutex_attr;
	pthread_mutexattr_init(&mutex_attr);
	pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_RECURSIVE);
	pthread_mutex_init(&stor_info->Mutex, &mutex_attr);
	pthread_mutexattr_destroy(&mutex_attr);
	pthread_cond_init(&stor_info->Cond, NULL);

	stor_info->Operation = Operation;
	globus_gridftp_server_get_block_size(Operation, &stor_info->BlockSize);
	globus_range_list_init(&stor_info->RangeList);
	result = stor_fill_range_list(stor_info->RangeList, TransferInfo);
	if (result != GLOBUS_SUCCESS)
	{
		globus_range_list_destroy(stor_info->RangeList);
		pthread_mutex_destroy(&stor_info->Mutex);
		pthread_cond_destroy(&stor_info->Cond);
		globus_free(stor_info);
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}

	// Initialize PIO
	result = pio_init(&stor_info->Pio);
	if (result != GLOBUS_SUCCESS)
	{
		globus_range_list_destroy(stor_info->RangeList);
		pthread_mutex_destroy(&stor_info->Mutex);
		pthread_cond_destroy(&stor_info->Cond);
		globus_free(stor_info);
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}

	globus_gridftp_server_begin_transfer(Operation, 0, NULL);

	pthread_mutex_lock(&stor_info->Mutex);
	{
		// Fire off GridFTP reads
		result = stor_register_gridftp_reads(stor_info, NULL, 0);
		if (result != GLOBUS_SUCCESS)
		{
			pio_cancel(stor_info->Pio);
			pio_destroy(stor_info->Pio);
			globus_list_destroy_all(stor_info->BufferList, free);
			globus_range_list_destroy(stor_info->RangeList);
			pthread_mutex_unlock(&stor_info->Mutex);
			pthread_mutex_destroy(&stor_info->Mutex);
			pthread_cond_destroy(&stor_info->Cond);
			globus_free(stor_info);
			globus_gridftp_server_finished_transfer(Operation, result);
			return;
		}

		// Start PIO
		result = pio_start(stor_info->Pio);
		if (result != GLOBUS_SUCCESS)
		{
			pio_cancel(stor_info->Pio);
			pio_destroy(stor_info->Pio);
			globus_list_destroy_all(stor_info->BufferList, free);
			globus_range_list_destroy(stor_info->RangeList);
			pthread_mutex_unlock(&stor_info->Mutex);
			pthread_mutex_destroy(&stor_info->Mutex);
			pthread_cond_destroy(&stor_info->Cond);
			globus_free(stor_info);
			globus_gridftp_server_finished_transfer(Operation, result);
			return;
		}
	}
	pthread_mutex_unlock(&stor_info->Mutex);

	return;
}

void
stor_pio_callback(globus_result_t   Result,
                  char            * Buffer,
                  uint32_t          Length,
                  void            * CallbackArg)
{
	stor_info_t   * stor_info = CallbackArg;
	globus_result_t result    = GLOBUS_SUCCESS;

	pthread_mutex_lock(&stor_info->Mutex);
	{
		result = stor_register_gridftp_reads(stor_info, Buffer, Length);
	}
	pthread_mutex_unlock(&stor_info->Mutex);
}

void
stor_gridfp_callback(globus_gfs_operation_t  Operation,
                     globus_result_t         Result,
                     globus_byte_t         * Buffer,
                     globus_size_t           Length,
                     globus_off_t            Offset,
                     globus_bool_t           Eof,
                     void                  * CallbackArg)
{
	stor_info_t   * stor_info = CallbackArg;
	globus_result_t result    = GLOBUS_SUCCESS;

	pthread_mutex_lock(&stor_info->Mutex);
	{
		/* Remove this range from our expected range list. */
		globus_range_list_remove(stor_info->RangeList, Offset, Length);

		/* Send this buffer to PIO. */
		result = pio_register_write(stor_info->Pio, 
		                            (char *)Buffer, 
		                            Offset, 
		                            Length,
		                            stor_pio_callback,
		                            stor_info);

		if (Eof)
		{
			if (globus_range_list_size(stor_info->RangeList) == 0)
			{
				result = pio_destroy(stor_info->Pio);
				globus_list_destroy_all(stor_info->BufferList, free);
				globus_range_list_destroy(stor_info->RangeList);
				pthread_mutex_unlock(&stor_info->Mutex);
				pthread_mutex_destroy(&stor_info->Mutex);
				pthread_cond_destroy(&stor_info->Cond);
				globus_free(stor_info);
				globus_gridftp_server_finished_transfer(Operation, result);
				return;
			}
		}

		result = stor_register_gridftp_reads(stor_info, NULL, 0);
	}
	pthread_mutex_unlock(&stor_info->Mutex);
}


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
#include "stor.h"
#include "pio.h"

int
stor_pio_callout(char     * Buffer,
                 uint32_t * Length,
                 uint64_t   Offset,
                 void     * CallbackArg);

void
stor_pio_completion_callback(globus_result_t Result,
                             void          * UserArg);
globus_result_t
stor_can_change_cos(char * Pathname, int * can_change_cos)
{
	int retval;
	hpss_fileattr_t fileattr;
	ns_FilesetAttrBits_t fileset_attr_bits;
	ns_FilesetAttrs_t    fileset_attr;

	GlobusGFSName(stor_can_change_cos);

	memset(&fileattr, 0, sizeof(hpss_fileattr_t));
	retval = hpss_FileGetAttributes(Pathname, &fileattr);
	if (retval)
		return GlobusGFSErrorSystemError("hpss_FileGetAttributes", -retval);

	fileset_attr_bits = orbit64m(0, NS_FS_ATTRINDEX_COS);
	memset(&fileset_attr, 0, sizeof(ns_FilesetAttrs_t));
	retval = hpss_FilesetGetAttributes(NULL,
	                                   &fileattr.Attrs.FilesetId,
	                                   NULL,
	                                   NULL,
	                                   fileset_attr_bits,
	                                   &fileset_attr);
	if (retval)
		return GlobusGFSErrorSystemError("hpss_FilesetGetAttributes", -retval);

	*can_change_cos = !fileset_attr.ClassOfService;
	return GLOBUS_SUCCESS;
}

globus_result_t
stor_open_for_writing(char        * Pathname,
                      globus_off_t  AllocSize,
                      globus_bool_t Truncate,
                      int         * FileFD,
                      int         * FileStripeWidth)
{
	int                     oflags      = 0;
	int                     retval      = 0;
	int                     can_change_cos = 0;
	globus_off_t            file_length = 0;
	globus_result_t         result      = GLOBUS_SUCCESS;
	hpss_cos_hints_t        hints_in;
	hpss_cos_hints_t        hints_out;
	hpss_cos_priorities_t   priorities;

	GlobusGFSName(stor_open_for_writing);

	*FileFD = -1;

	/* Initialize the hints in. */
	memset(&hints_in, 0, sizeof(hpss_cos_hints_t));

	/* Initialize the hints out. */
	memset(&hints_out, 0, sizeof(hpss_cos_hints_t));

	/* Initialize the priorities. */
	memset(&priorities, 0, sizeof(hpss_cos_priorities_t));

	/*
	 * If this is a new file we need to determine the class of service
	 * by either:
	 *  1) set explicitly within the session handle or
	 *  2) determined by the size of the incoming file
	 */
	if (Truncate == GLOBUS_TRUE)
	{
		if (AllocSize != 0)
		{
			/*
			 * Use the ALLO size.
			 */
			file_length = AllocSize;
			CONVERT_LONGLONG_TO_U64(file_length, hints_in.MinFileSize);
			CONVERT_LONGLONG_TO_U64(file_length, hints_in.MaxFileSize);
			priorities.MinFileSizePriority = REQUIRED_PRIORITY;
			/*
			 * If MaxFileSizePriority is required, you can not place
			 *  a file into a COS where it's max size is < the size
			 *  of this file regardless of whether or not enforce max
			 *  is enabled on the COS (it doesn't even try).
			 */
			priorities.MaxFileSizePriority = HIGHLY_DESIRED_PRIORITY;
		}
    }

	/* Determine the open flags. */
	oflags = O_WRONLY;
	if (Truncate == GLOBUS_TRUE)
		oflags |= O_CREAT|O_TRUNC;

	/* Open the HPSS file. */
	*FileFD = hpss_Open(Pathname,
	                    oflags,
	                    S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH,
	                    &hints_in,
	                    &priorities,
	                    &hints_out);
	if (*FileFD < 0)
	{
		result = GlobusGFSErrorSystemError("hpss_Open", -(*FileFD));
		goto cleanup;
	}

	result = stor_can_change_cos(Pathname, &can_change_cos);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Handle the case of the file that already existed. */
	if (Truncate == GLOBUS_TRUE && can_change_cos)
	{
		hpss_cos_md_t cos_md;

		retval = hpss_SetCOSByHints(*FileFD,
		                            0,
		                            &hints_in,
		                            &priorities,
		                            &cos_md);

		if (retval)
		{
			result = GlobusGFSErrorSystemError("hpss_SetCOSByHints", -(retval));
			goto cleanup;
		}
	}

	/* Copy out the file stripe width. */
	*FileStripeWidth = hints_out.StripeWidth;

cleanup:
	if (result)
	{
		if (*FileFD != -1)
			hpss_Close(*FileFD);
		*FileFD = -1;
	}

	return result;
}

void
stor(globus_gfs_operation_t       Operation,
     globus_gfs_transfer_info_t * TransferInfo)
{
	stor_info_t   * stor_info         = NULL;
	globus_result_t result            = GLOBUS_SUCCESS;
	int             file_stripe_width = 0;

	GlobusGFSName(stor);

	/*
	 * Create our structure.
	 */
	stor_info = malloc(sizeof(stor_info_t));
	if (!stor_info)
	{
		result = GlobusGFSErrorMemory("stor_info_t");
		goto cleanup;
	}
	memset(stor_info, 0, sizeof(stor_info_t));
	stor_info->Operation    = Operation;
	stor_info->TransferInfo = TransferInfo;
	stor_info->FileFD       = -1;
	pthread_mutex_init(&stor_info->Mutex, NULL);
	pthread_cond_init(&stor_info->Cond, NULL);

	globus_gridftp_server_get_block_size(Operation, &stor_info->BlockSize);

	/*
	 * Open the file.
	 */
	result = stor_open_for_writing(TransferInfo->pathname,
	                               TransferInfo->alloc_size,
	                               TransferInfo->truncate,
	                               &stor_info->FileFD,
	                               &file_stripe_width);
	if (result) goto cleanup;

	/*
	 * Setup PIO
	 */
	result = pio_start(HPSS_PIO_WRITE,
	                   stor_info->FileFD,
	                   file_stripe_width,
	                   stor_info->BlockSize,
	                   TransferInfo->alloc_size,
	                   stor_pio_callout,
	                   stor_pio_completion_callback,
	                   stor_info);

cleanup:
	if (result)
	{
		globus_gridftp_server_finished_transfer(Operation, result);
		if (stor_info)
		{
			if (stor_info->FileFD != -1)
				hpss_Close(stor_info->FileFD);
			pthread_mutex_destroy(&stor_info->Mutex);
			pthread_cond_destroy(&stor_info->Cond);
			free(stor_info);
		}
	}
}

void
stor_gridftp_callout(globus_gfs_operation_t Operation,
                     globus_result_t        Result,
                     globus_byte_t        * Buffer,
                     globus_size_t          Length,
                     globus_off_t           Offset,
                     globus_bool_t          Eof,
                     void                 * UserArg)
{
	stor_info_t * stor_info = UserArg;

	pthread_mutex_lock(&stor_info->Mutex);
	{
		if (Eof) stor_info->Eof = Eof;
		if (Result && !stor_info->Result) stor_info->Result = Result;
		if (!Result) stor_info->Length = Length;
if (Length)
assert(Offset  == stor_info->Offset);
assert(Length  <= stor_info->BlockSize);
		pthread_cond_signal(&stor_info->Cond);
	}
	pthread_mutex_unlock(&stor_info->Mutex);
}

int
stor_pio_callout(char     * Buffer,
                 uint32_t * Length,
                 uint64_t   Offset,
                 void     * CallbackArg)
{
	int             rc        = 0;
	stor_info_t   * stor_info = CallbackArg;
	globus_result_t result    = GLOBUS_SUCCESS;

	GlobusGFSName(stor_pio_callout);

	pthread_mutex_lock(&stor_info->Mutex);
	{
assert(Offset == stor_info->Offset);
assert(*Length <= stor_info->BlockSize);
		if (stor_info->Eof)
		{
			rc = 1; /* Signal to shutdown. */
			if (!stor_info->Result)
				stor_info->Result = GlobusGFSErrorGeneric("Premature end of data transfer");
			goto cleanup;
		}

		if (!stor_info->Started)
			globus_gridftp_server_begin_transfer(stor_info->Operation, 0, NULL);
		stor_info->Started = 1;

		result = globus_gridftp_server_register_read(stor_info->Operation,
		                                             (globus_byte_t *)Buffer,
		                                             *Length,
		                                             stor_gridftp_callout,
		                                             stor_info);

		if (result)
		{
			rc = 1; /* Signal to shutdown. */
			if (!stor_info->Result) stor_info->Result = result;
			goto cleanup;
		}

		pthread_cond_wait(&stor_info->Cond, &stor_info->Mutex);

		if (stor_info->Result)
		{
			rc = 1; /* Signal to shutdown. */
			goto cleanup;
		}

		*Length = stor_info->Length;
		stor_info->Offset += *Length;
	}
cleanup:
	pthread_mutex_unlock(&stor_info->Mutex);

	return rc;
}

void
stor_pio_completion_callback (globus_result_t Result,
                              void          * UserArg)
{
	globus_result_t result    = Result;
	stor_info_t   * stor_info = UserArg;
	int             rc        = 0;

	GlobusGFSName(stor_pio_completion_callback);

	/* Give our error priority. */
	if (stor_info->Result)
		result = stor_info->Result;

	rc = hpss_Close(stor_info->FileFD);
	if (rc && !result)
		result = GlobusGFSErrorSystemError("hpss_Close", -rc);

	globus_gridftp_server_finished_transfer(stor_info->Operation, result);

	pthread_mutex_destroy(&stor_info->Mutex);
	pthread_cond_destroy(&stor_info->Cond);
	free(stor_info);
}


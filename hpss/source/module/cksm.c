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
#include "cksm.h"
#include "pio.h"

int
cksm_pio_callout(char     * Buffer,
                 uint32_t * Length,
                 uint64_t   Offset,
                 void     * CallbackArg);

void
cksm_pio_completion_callback(globus_result_t Result,
                             void          * UserArg);

globus_result_t
cksm_open_for_reading(char * Pathname,
	                  int  * FileFD,
	                  int  * FileStripeWidth)
{
	hpss_cos_hints_t      hints_in;
	hpss_cos_hints_t      hints_out;
	hpss_cos_priorities_t priorities;

	GlobusGFSName(cksm_open_for_reading);

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
cksm(globus_gfs_operation_t      Operation,
     globus_gfs_command_info_t * CommandInfo,
     commands_callback           Callback)
{
	globus_result_t result            = GLOBUS_SUCCESS;
	cksm_info_t   * cksm_info         = NULL;
	int             rc                = 0;
	int             file_stripe_width = 0;
	hpss_stat_t     hpss_stat_buf;

	GlobusGFSName(cksm);

	/*
	 * XXX Partial checksums not supported.
	 */
	if (CommandInfo->cksm_offset != 0 || CommandInfo->cksm_length != -1)
	{
		result = GlobusGFSErrorGeneric("Partial checksums not supported");
		Callback(Operation, result, NULL);
		return;
	}

	rc = hpss_Stat(CommandInfo->pathname, &hpss_stat_buf);
	if (rc)
	{
		result = GlobusGFSErrorSystemError("hpss_Stat", -rc);
		Callback(Operation, result, NULL);
		return;
	}

	cksm_info = malloc(sizeof(cksm_info_t));
	if (!cksm_info)
	{
		result = GlobusGFSErrorMemory("cksm_info_t");
		goto cleanup;
	}
	memset(cksm_info, 0, sizeof(cksm_info_t));
	cksm_info->Operation   = Operation;
	cksm_info->CommandInfo = CommandInfo;
	cksm_info->Callback    = Callback;
	cksm_info->FileFD      = -1;

	rc = MD5_Init(&cksm_info->MD5Context);
	if (rc != 1)
	{
		result = GlobusGFSErrorGeneric("Failed to create MD5 context");
		goto cleanup;
	}

	globus_gridftp_server_get_block_size(Operation, &cksm_info->BlockSize);

	/*
	 * Open the file.
	 */
	result = cksm_open_for_reading(CommandInfo->pathname,
	                               &cksm_info->FileFD,
	                               &file_stripe_width);
	if (result) goto cleanup;

	/*
	 * Setup PIO
	 */
	result = pio_start(HPSS_PIO_READ,
	                   cksm_info->FileFD,
	                   file_stripe_width,
	                   cksm_info->BlockSize,
	                   hpss_stat_buf.st_size,
	                   cksm_pio_callout,
	                   cksm_pio_completion_callback,
	                   cksm_info);

cleanup:
	if (result)
	{
		if (cksm_info)
		{
			if (cksm_info->FileFD != -1)
				hpss_Close(cksm_info->FileFD);
			free(cksm_info);
		}
		Callback(Operation, result, NULL);
	}
}

int
cksm_pio_callout(char     * Buffer,
                 uint32_t * Length,
                 uint64_t   Offset,
                 void     * CallbackArg)
{
	int           rc        = 0;
	cksm_info_t * cksm_info = CallbackArg;

	GlobusGFSName(cksm_pio_callout);

assert(Offset == cksm_info->Offset);
assert(*Length <= cksm_info->BlockSize);

	rc = MD5_Update(&cksm_info->MD5Context, Buffer, *Length);
	if (rc != 1)
	{
		cksm_info->Result = GlobusGFSErrorGeneric("MD5_Update() failed");
		return 1;
	}
cksm_info->Offset += *Length;
	return 0;
}

void
cksm_pio_completion_callback(globus_result_t Result, void * UserArg)
{
	globus_result_t result    = Result;
	cksm_info_t   * cksm_info = UserArg;
	int             rc        = 0;
	unsigned char   md5_digest[MD5_DIGEST_LENGTH];
	char            cksm_string[2*MD5_DIGEST_LENGTH+1];
	int             i;

	GlobusGFSName(cksm_pio_completion_callback);

	/* Give our error priority. */
	if (cksm_info->Result)
		result = cksm_info->Result;

	rc = hpss_Close(cksm_info->FileFD);
	if (rc && !result)
		result = GlobusGFSErrorSystemError("hpss_Close", -rc);

	if (!result)
	{
		rc = MD5_Final(md5_digest, &cksm_info->MD5Context);
		if (rc != 1)
			result = GlobusGFSErrorGeneric("MD5_Final() failed");
	}

	if (!result)
	{
		for (i = 0; i < MD5_DIGEST_LENGTH; i++)
		{
			sprintf(&(cksm_string[i*2]), "%02x", (unsigned int)md5_digest[i]);
		}
	}

	cksm_info->Callback(cksm_info->Operation, result, result ? NULL : cksm_string);

	free(cksm_info);
}


/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2012 NCSA.  All rights reserved.
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
 * System includes.
 */
#include <openssl/md5.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * HPSS includes.
 */
#include <hpss_hash.h>
#include <hpss_api.h>
#include <hpss_xml.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_range_list.h"
#include "gridftp_dsi_hpss_checksum.h"
#include "gridftp_dsi_hpss_buffer.h"
#include "gridftp_dsi_hpss_misc.h"

struct checksum {
	MD5_CTX                   MD5Context;
	buffer_handle_t         * BufferHandle;
	buffer_priv_id_t          PrivateBufferID;
	checksum_eof_callback_t   EofCallbackFunc;
	void                    * EofCallbackArg;
	checksum_buffer_pass_t    BufferPassFunc;
	void                    * BufferPassArg;

	globus_mutex_t            Lock;
	globus_cond_t             Cond;
	globus_result_t           Result;
	globus_bool_t             Stop;
	range_list_t            * RangeList;
	int                       ThreadCount;
};

globus_result_t
checksum_init(buffer_handle_t           *  BufferHandle,
              globus_gfs_command_info_t *  CommandInfo,
              checksum_eof_callback_t      EofCallbackFunc,
              void                      *  EofCallbackArg,
              checksum_t                ** Checksum)
{
	int             retval = 0;
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the handle. */
	*Checksum = (checksum_t *) globus_calloc(1, sizeof(checksum_t));
	if (*Checksum == NULL)
	{
		result = GlobusGFSErrorMemory("checksum_t");
		goto cleanup;
	}

	retval = MD5_Init(&(*Checksum)->MD5Context);
	if (retval == 0)
	{
		result = GlobusGFSErrorGeneric("Failed to create MD5 context");
		goto cleanup;
	}

	(*Checksum)->BufferHandle    = BufferHandle;
	(*Checksum)->EofCallbackFunc = EofCallbackFunc;
	(*Checksum)->EofCallbackArg  = EofCallbackArg;
	(*Checksum)->Result          = GLOBUS_SUCCESS;
	(*Checksum)->Stop            = GLOBUS_FALSE;
	(*Checksum)->ThreadCount     = 0;
	(*Checksum)->PrivateBufferID = buffer_create_private_list(BufferHandle);

	globus_mutex_init(&(*Checksum)->Lock, NULL);
	globus_cond_init(&(*Checksum)->Cond, NULL);

	/* Create the range list. */
	result = range_list_init(&(*Checksum)->RangeList);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Populate the range list with our ranges. */
	result = range_list_fill_cksm_range((*Checksum)->RangeList, CommandInfo);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

void
checksum_set_buffer_pass_func(checksum_t            * Checksum,
                              checksum_buffer_pass_t  BufferPassFunc,
                              void                  * BufferPassArg)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	Checksum->BufferPassFunc = BufferPassFunc;
	Checksum->BufferPassArg  = BufferPassArg;

	GlobusGFSHpssDebugExit();
}

void
checksum_destroy(checksum_t * Checksum)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (Checksum != NULL)
	{
		/* Destroy the range list. */
		range_list_destroy(Checksum->RangeList);

		globus_mutex_destroy(&Checksum->Lock);
		globus_cond_destroy(&Checksum->Cond);
		globus_free(Checksum);
	}

	GlobusGFSHpssDebugExit();
}

void
checksum_buffer(void         * CallbackArg,
                char         * Buffer,
                globus_off_t   Offset,
                globus_off_t   Length)
{
	int             retval        = 0;
	globus_bool_t   call_callback = GLOBUS_FALSE;
	globus_bool_t   stop          = GLOBUS_FALSE;
	globus_bool_t   empty         = GLOBUS_FALSE;
	globus_result_t result        = GLOBUS_SUCCESS;
	globus_off_t    range_offset  = 0;
	globus_off_t    range_length  = 0;
	globus_off_t    buffer_length = 0;
	checksum_t    * checksum      = (checksum_t *) CallbackArg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Put the buffer on our the ready list. */
	buffer_store_ready_buffer(checksum->BufferHandle,
	                          checksum->PrivateBufferID,
	                          Buffer,
	                          Offset,
	                          Length);

	globus_mutex_lock(&checksum->Lock);
	{
		/* Indicate that we are here. */
		checksum->ThreadCount++;

		/* Save the error. */
		result = checksum->Result;

		/* Save the stop flag. */
		stop = checksum->Stop;
	}
	globus_mutex_unlock(&checksum->Lock);

	while (GLOBUS_TRUE)
	{
		if (result != GLOBUS_SUCCESS)
			break;

		if (stop == GLOBUS_TRUE)
			break;

		Buffer = NULL;

		globus_mutex_lock(&checksum->Lock);
		{
			empty = range_list_empty(checksum->RangeList);

			/* Peek at the next offset we need. */
			if (empty == GLOBUS_FALSE)
				range_list_peek(checksum->RangeList, &range_offset, &range_length);
		}
		globus_mutex_unlock(&checksum->Lock);

		if (empty == GLOBUS_TRUE)
			break;

		/* Try to get the next ready buffer. */
		buffer_get_ready_buffer(checksum->BufferHandle,
		                        checksum->PrivateBufferID,
		                        &Buffer, 
		                        range_offset, 
		                        &buffer_length);

		if (Buffer == NULL)
			break;

		retval = MD5_Update(&checksum->MD5Context, Buffer, buffer_length);
		if (retval == 0)
		{
			globus_mutex_lock(&checksum->Lock);
			{
				if (checksum->Result == GLOBUS_SUCCESS)
				{
					result = GlobusGFSErrorSystemError("hpss_HashAppendBuf", -retval);
					checksum->Result = result;
					call_callback = GLOBUS_TRUE;
				}
			}
			globus_mutex_unlock(&checksum->Lock);

			/* Inform the upper layer. */
			if (call_callback == GLOBUS_TRUE)
				checksum->EofCallbackFunc(checksum->EofCallbackArg, result);
				                          
			break;
		}

		/* Pass the buffer forward. */
		checksum->BufferPassFunc(checksum->BufferPassArg, Buffer, range_offset, buffer_length);

		/* Remove this range from our range list. */
		range_list_delete(checksum->RangeList, range_offset, buffer_length);
	}

	globus_mutex_lock(&checksum->Lock);
	{
		/* Indicate that we are leaving. */
		checksum->ThreadCount--;

		/* Check if we should wake someone. */
		if (checksum->ThreadCount == 0)
		{
			globus_cond_signal(&checksum->Cond);
		}
	}
	globus_mutex_unlock(&checksum->Lock);

	GlobusGFSHpssDebugExit();
}

/* May need to call the hashing function ourselves b/c of gaps on 
   error after flusing started.
 */
/*
 * Called after lower layers have been flushed. We should not be
 * recieving any new buffers.
 */
void
checksum_flush(checksum_t * Checksum)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (Checksum != NULL)
	{
		globus_mutex_lock(&Checksum->Lock);
		{
			while (buffer_get_ready_buffer_count(Checksum->BufferHandle,
			                                     Checksum->PrivateBufferID) > 0)
			{
				if (Checksum->Result != GLOBUS_SUCCESS)
					break;

				globus_cond_wait(&Checksum->Cond, &Checksum->Lock);
			}
		}
		globus_mutex_unlock(&Checksum->Lock);
	}

	GlobusGFSHpssDebugExit();
/* If we are here w/o error, we have a valid checksum. */
}

/*
 * The important thing is that once we set the stop flag, our buffer
 * pass function should not be passing buffers, although it may
 * continue to receive buffers.
 */
void
checksum_stop(checksum_t * Checksum)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (Checksum != NULL)
	{
		globus_mutex_lock(&Checksum->Lock);
		{
			Checksum->Stop = GLOBUS_TRUE;

			while (Checksum->ThreadCount > 0)
			{
				globus_cond_wait(&Checksum->Cond, &Checksum->Lock);
			}
		}
		globus_mutex_unlock(&Checksum->Lock);
	}

	GlobusGFSHpssDebugExit();
}

globus_result_t
checksum_get_sum(checksum_t * Checksum, char ** ChecksumString)
{
	int             index  = 0;
	int             retval = 0;
	globus_result_t result = GLOBUS_SUCCESS;
	unsigned char   md5_digest[MD5_DIGEST_LENGTH];

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	*ChecksumString = (char *) malloc(2*MD5_DIGEST_LENGTH + 1);

	retval = MD5_Final(md5_digest, &Checksum->MD5Context);
globus_assert(retval == 1);
	for (index = 0; index < MD5_DIGEST_LENGTH; index++)
	{
		sprintf(&((*ChecksumString)[index*2]), "%02x", (unsigned int)md5_digest[index]);
	}

	GlobusGFSHpssDebugExit();
	return result;
}


/*
 * /hpss/user/cksum/algorithm                                  md5
 * /hpss/user/cksum/checksum               93b885adfe0da089cdf634904fd59f71
 * /hpss/user/cksum/lastupdate                          1376424299
 * /hpss/user/cksum/errors                                       0
 * /hpss/user/cksum/state                                    Valid
 * /hpss/user/cksum/app                                    hpsssum
 * /hpss/user/cksum/filesize                                     1
 */

static globus_result_t
_set_uda_on_file(char * File, char * Key, char * Value)
{
	int retval = 0;
	hpss_userattr_list_t attr_list;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	attr_list.len  = 1;
	attr_list.Pair = malloc(sizeof(hpss_userattr_t));
	if (!attr_list.Pair)
		return GlobusGFSErrorMemory("hpss_userattr_t");

	attr_list.Pair[0].Key = Key;
	attr_list.Pair[0].Value = Value;

	retval = hpss_UserAttrSetAttrs(File, &attr_list, NULL);
	free(attr_list.Pair);
	if (retval)
		return GlobusGFSErrorSystemError("hpss_UserAttrSetAttrs", -retval);

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static globus_result_t
_get_uda_on_file(char * File, char * Key, char * Value)
{
	int    retval = 0;
	char * tmp = NULL;
	char   xml[HPSS_XML_SIZE];
	hpss_userattr_list_t attr_list;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	*Value = '\0';

	attr_list.len  = 1;
	attr_list.Pair = malloc(sizeof(hpss_userattr_t));
	if (!attr_list.Pair)
		return GlobusGFSErrorMemory("hpss_userattr_t");

	attr_list.Pair[0].Key   = Key;
	attr_list.Pair[0].Value = xml;

	retval = hpss_UserAttrGetAttrs(File, &attr_list, UDA_API_VALUE);
	free(attr_list.Pair);
	if (retval == -ENOENT)
		return GLOBUS_SUCCESS;

	if (retval != -ENOENT)
	{
		if (retval)
			return GlobusGFSErrorSystemError("hpss_UserAttrGetAttrs", -retval);

		tmp = hpss_ChompXMLHeader(xml, NULL);
		if (tmp)
		{
			strcpy(Value, tmp);
			free(tmp);
		}
	}


	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

static globus_result_t
_remove_uda_on_file(char * File, char * Key)
{
	int retval = 0;
	hpss_userattr_list_t attr_list;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	attr_list.len  = 1;
	attr_list.Pair = malloc(sizeof(hpss_userattr_t));
	if (!attr_list.Pair)
		return GlobusGFSErrorMemory("hpss_userattr_t");

	attr_list.Pair[0].Key   = Key;
	attr_list.Pair[0].Value = "1";

	retval = hpss_UserAttrDeleteAttrs(File, &attr_list, NULL);
	free(attr_list.Pair);

	if (retval && retval != -ENOENT)
		return GlobusGFSErrorSystemError("hpss_UserAttrDeleteAttrs()", -retval);

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

/*
 * /hpss/user/cksum/algorithm                                  md5
 * /hpss/user/cksum/checksum               93b885adfe0da089cdf634904fd59f71
 * /hpss/user/cksum/lastupdate                          1376424299
 * /hpss/user/cksum/errors                                       0
 * /hpss/user/cksum/state                                    Valid
 * /hpss/user/cksum/app                                    hpsssum
 * /hpss/user/cksum/filesize                                     1
 */
globus_result_t
checksum_set_file_sum(char * File, char * ChecksumString)
{
	char buf[32];
	globus_result_t result   = GLOBUS_SUCCESS;
	globus_off_t    filesize = 0;

	result = misc_get_file_size(File, &filesize);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	result = _set_uda_on_file(File, "/hpss/user/cksum/algorithm",  "md5");
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	result = _set_uda_on_file(File, "/hpss/user/cksum/checksum",   ChecksumString);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	snprintf(buf, sizeof(buf), "%lu", time(NULL));
	result = _set_uda_on_file(File, "/hpss/user/cksum/lastupdate", buf);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	result = _set_uda_on_file(File, "/hpss/user/cksum/errors",     "0");
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	result = _set_uda_on_file(File, "/hpss/user/cksum/state",      "Valid");
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	result = _set_uda_on_file(File, "/hpss/user/cksum/app",        "GridFTP");
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	snprintf(buf, sizeof(buf), "%lu", filesize);
	result = _set_uda_on_file(File, "/hpss/user/cksum/filesize",   buf);

cleanup:
	if (result != GLOBUS_SUCCESS)
		checksum_clear_file_sum(File);

	return result;
}

globus_result_t
checksum_get_file_sum(char * File, char ** ChecksumString)
{
	char value[64];
	globus_result_t result = GLOBUS_SUCCESS;

	memset(value, 0, sizeof(value));
	result = _get_uda_on_file(File, "/hpss/user/cksum/state", value);
	if (result)
		return result;
	if (strcmp(value, "Valid") != 0)
		return GLOBUS_SUCCESS;

	memset(value, 0, sizeof(value));
	result = _get_uda_on_file(File, "/hpss/user/cksum/algorithm", value);
	if (result)
		return result;
	if (strcmp(value, "md5") != 0)
		return GLOBUS_SUCCESS;

	memset(value, 0, sizeof(value));
	result = _get_uda_on_file(File, "/hpss/user/cksum/checksum", value);
	if (result)
		return result;

	*ChecksumString = strdup(value);
	return GLOBUS_SUCCESS;
}

globus_result_t
checksum_clear_file_sum(char * File)
{
	globus_result_t result = GLOBUS_SUCCESS;

	result = _remove_uda_on_file(File, "/hpss/user/cksum/algorithm");
	if (result != GLOBUS_SUCCESS)
		return result;

	result = _remove_uda_on_file(File, "/hpss/user/cksum/checksum");
	if (result != GLOBUS_SUCCESS)
		return result;

	result = _remove_uda_on_file(File, "/hpss/user/cksum/lastupdate");
	if (result != GLOBUS_SUCCESS)
		return result;

	result = _remove_uda_on_file(File, "/hpss/user/cksum/errors");
	if (result != GLOBUS_SUCCESS)
		return result;

	result = _remove_uda_on_file(File, "/hpss/user/cksum/state");
	if (result != GLOBUS_SUCCESS)
		return result;

	result = _remove_uda_on_file(File, "/hpss/user/cksum/app");
	if (result != GLOBUS_SUCCESS)
		return result;

	return _remove_uda_on_file(File, "/hpss/user/cksum/filesize");
}


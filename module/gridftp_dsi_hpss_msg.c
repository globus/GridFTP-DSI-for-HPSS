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
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_misc.h"
#include "gridftp_dsi_hpss_msg.h"

typedef struct msg_handler {
	globus_mutex_t    Lock;
	globus_cond_t     Cond;
	globus_bool_t     InUse;
	int               CurrentCallers;
	msg_recv_func_t   MsgRecvFunc;
	void            * MsgRecvFuncArg;
} msg_handler_t;

struct msg_handle {
	msg_handler_t SrcCompHandlers[MSG_COMP_ID_COUNT][MSG_COMP_ID_COUNT];
	msg_handler_t DstCompHandlers[MSG_COMP_ID_COUNT][MSG_COMP_ID_COUNT];
};

struct msg_register_id {
	msg_handler_t * SrcCompHandlers[MSG_COMP_ID_COUNT];
	msg_handler_t * DstCompHandlers[MSG_COMP_ID_COUNT];
};

static void
msg_validate_msg_comp_id_count()
{
	int comp_id_count = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Count the number of message component IDs. */
	for (comp_id_count = 0; (MSG_COMP_ID_MASK >> comp_id_count) != 0; comp_id_count++);

	globus_assert(comp_id_count == MSG_COMP_ID_COUNT);

	GlobusGFSHpssDebugExit();
}

static int
msg_count_comp_ids(msg_comp_id_t MsgCompIDs)
{
	int index         = 0;
	int comp_id_count = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Count the number of message component IDs. */
	for (index = 0; index < MSG_COMP_ID_COUNT; index++)
	{
		if ((MsgCompIDs >> index) == 1)
			comp_id_count++;
	}

	GlobusGFSHpssDebugExit();
	return comp_id_count;
}

/*
 * Initialize the message handle. The handle is shared by
 * all components.
 */
globus_result_t
msg_init(msg_handle_t ** MsgHandle)
{
	globus_result_t result = GLOBUS_SUCCESS;
	int             index1 = 0;
	int             index2 = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Sanity check out msg component IDs. */
	msg_validate_msg_comp_id_count();

	/* Allocate the handle. */
	*MsgHandle = (msg_handle_t *) globus_calloc(1, sizeof(msg_handle_t));
	if (*MsgHandle == NULL)
	{
		result = GlobusGFSErrorMemory("msg_handle_t");
		goto cleanup;
	}

	/* Initialize the source handlers. */
	for (index1 = 0; index1 < MSG_COMP_ID_COUNT; index1++)
	{
		for (index2 = 0; index2 < MSG_COMP_ID_COUNT; index2++)
		{
			globus_mutex_init(&(*MsgHandle)->SrcCompHandlers[index1][index2].Lock, NULL);
			globus_cond_init(&(*MsgHandle)->SrcCompHandlers[index1][index2].Cond, NULL);
			(*MsgHandle)->SrcCompHandlers[index1][index2].InUse          = GLOBUS_FALSE;
			(*MsgHandle)->SrcCompHandlers[index1][index2].CurrentCallers = 0;
			(*MsgHandle)->SrcCompHandlers[index1][index2].MsgRecvFunc    = NULL;
			(*MsgHandle)->SrcCompHandlers[index1][index2].MsgRecvFuncArg = NULL;
		}
	}

	/* Initialize the destination handlers. */
	for (index1 = 0; index1 < MSG_COMP_ID_COUNT; index1++)
	{
		for (index2 = 0; index2 < MSG_COMP_ID_COUNT; index2++)
		{
			globus_mutex_init(&(*MsgHandle)->DstCompHandlers[index1][index2].Lock, NULL);
			globus_cond_init(&(*MsgHandle)->DstCompHandlers[index1][index2].Cond, NULL);
			(*MsgHandle)->DstCompHandlers[index1][index2].InUse          = GLOBUS_FALSE;
			(*MsgHandle)->DstCompHandlers[index1][index2].CurrentCallers = 0;
			(*MsgHandle)->DstCompHandlers[index1][index2].MsgRecvFunc    = NULL;
			(*MsgHandle)->DstCompHandlers[index1][index2].MsgRecvFuncArg = NULL;
		}
	}

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

/*
 * Destroy the message handle.
 */
void
msg_destroy(msg_handle_t * MsgHandle)
{
	int             index1 = 0;
	int             index2 = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (MsgHandle != NULL)
	{
		/* Initialize the source handlers. */
		for (index1 = 0; index1 < MSG_COMP_ID_COUNT; index1++)
		{
			for (index2 = 0; index2 < MSG_COMP_ID_COUNT; index2++)
			{
				globus_mutex_destroy(&MsgHandle->SrcCompHandlers[index1][index2].Lock);
				globus_cond_destroy(&MsgHandle->SrcCompHandlers[index1][index2].Cond);
			}
		}

		/* Initialize the destination handlers. */
		for (index1 = 0; index1 < MSG_COMP_ID_COUNT; index1++)
		{
			for (index2 = 0; index2 < MSG_COMP_ID_COUNT; index2++)
			{
				globus_mutex_destroy(&MsgHandle->DstCompHandlers[index1][index2].Lock);
				globus_cond_destroy(&MsgHandle->DstCompHandlers[index1][index2].Cond);
			}
		}
		globus_free(MsgHandle);
	}

	GlobusGFSHpssDebugExit();
}

/*
 * Caller will receive messages that:
 *   1) Sender specifies SrcMsgCompID in SrcMsgCompIDs and DstMsgCompID is MSG_COMP_ID_ANY
 *   2) Sender specifies DstMsgCompID in DstMsgCompIDs
 *
 * Either SrcMsgCompIDs or DstMsgCompIDs can be MSG_COMP_ID_NONE
 */
globus_result_t
msg_register(msg_handle_t      * MsgHandle,
             msg_comp_id_t       SrcMsgCompIDs,
             msg_comp_id_t       DstMsgCompIDs,
             msg_recv_func_t     MsgRecvFunc,
             void              * MsgRecvFuncArg,
             msg_register_id_t * MsgRegisterID)
{
	int             index1 = 0;
	int             index2 = 0;
	int             index3 = 0;
	globus_bool_t   found  = GLOBUS_FALSE;
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Validate our component IDs. */
	if (SrcMsgCompIDs != MSG_COMP_ID_NONE)
		globus_assert((SrcMsgCompIDs & ~MSG_COMP_ID_MASK) == 0);

	if (DstMsgCompIDs != MSG_COMP_ID_NONE)
		globus_assert((DstMsgCompIDs & ~MSG_COMP_ID_MASK) == 0);

	/* Allocate the returned ID */
	*MsgRegisterID = (msg_register_id_t)globus_calloc(1, sizeof(struct msg_register_id));
	if (*MsgRegisterID == NULL)
	{
		result = GlobusGFSErrorMemory("msg_register_id_t");
		goto cleanup;
	}

	index3 = 0;
	if (SrcMsgCompIDs != MSG_COMP_ID_NONE)
	{
		/* For each possible component ID... */
		for (index1 = 0; index1 < MSG_COMP_ID_COUNT; index1++)
		{
			/* If the call specified this ID... */
			if ((SrcMsgCompIDs & (1 << index1)) == 0)
				continue;

			/* Indicate that we have not yet found a spot for this id. */
			found = GLOBUS_FALSE;

			/* For each possible registration entry for this source id... */
			for (index2 = 0; index2 < MSG_COMP_ID_COUNT; index2++)
			{
				globus_mutex_lock(&MsgHandle->SrcCompHandlers[index1][index2].Lock);
				{
					if (MsgHandle->SrcCompHandlers[index1][index2].InUse == GLOBUS_FALSE)
					{
						/* Set it to in use. */
						MsgHandle->SrcCompHandlers[index1][index2].InUse = GLOBUS_TRUE;
						MsgHandle->SrcCompHandlers[index1][index2].MsgRecvFunc = MsgRecvFunc;
						MsgHandle->SrcCompHandlers[index1][index2].MsgRecvFuncArg = MsgRecvFuncArg;

						/* Save this entry in our list for cleanup later. */
						(*MsgRegisterID)->SrcCompHandlers[index3++] = &MsgHandle->SrcCompHandlers[index1][index2];

						/* We found an entry. */
						found = GLOBUS_TRUE;
					}
				}
				globus_mutex_unlock(&MsgHandle->SrcCompHandlers[index1][index2].Lock);

				if (found == GLOBUS_TRUE)
					break;
			}

			/* Make sure we found a spot. */
			globus_assert(found == GLOBUS_TRUE);
		}
	}

	index3 = 0;
	if (DstMsgCompIDs != MSG_COMP_ID_NONE)
	{
		/* For each possible component ID... */
		for (index1 = 0; index1 < MSG_COMP_ID_COUNT; index1++)
		{
			/* If the call specified this ID... */
			if ((DstMsgCompIDs & (1 << index1)) == 0)
				continue;

			/* Indicate that we have not yet found a spot for this id. */
			found = GLOBUS_FALSE;

			/* For each possible registration entry for this source id... */
			for (index2 = 0; index2 < MSG_COMP_ID_COUNT; index2++)
			{
				globus_mutex_lock(&MsgHandle->DstCompHandlers[index1][index2].Lock);
				{
					if (MsgHandle->DstCompHandlers[index1][index2].InUse == GLOBUS_FALSE)
					{
						/* Set it to in use. */
						MsgHandle->DstCompHandlers[index1][index2].InUse          = GLOBUS_TRUE;
						MsgHandle->DstCompHandlers[index1][index2].MsgRecvFunc    = MsgRecvFunc;
						MsgHandle->DstCompHandlers[index1][index2].MsgRecvFuncArg = MsgRecvFuncArg;

						/* Save this entry in our list for cleanup later. */
						(*MsgRegisterID)->DstCompHandlers[index3++] = &MsgHandle->DstCompHandlers[index1][index2];

						/* We found an entry. */
						found = GLOBUS_TRUE;
					}
				}
				globus_mutex_unlock(&MsgHandle->DstCompHandlers[index1][index2].Lock);

				if (found == GLOBUS_TRUE)
					break;
			}

			/* Make sure we found a spot. */
			globus_assert(found == GLOBUS_TRUE);
		}
	}
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
msg_unregister(msg_handle_t      * MsgHandle,
               msg_register_id_t   MsgRegisterID)
{
	int index = 0;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (MsgRegisterID != MSG_REGISTER_ID_NONE)
	{
		for (index = 0; index < MSG_COMP_ID_COUNT; index++)
		{
			if (MsgRegisterID->SrcCompHandlers[index] == NULL)
				break;

			globus_mutex_lock(&MsgRegisterID->SrcCompHandlers[index]->Lock);
			{
				/* Wait until it is not in use. */
				while (MsgRegisterID->SrcCompHandlers[index]->CurrentCallers > 0)
				{
					globus_cond_wait(&MsgRegisterID->SrcCompHandlers[index]->Cond,
					                 &MsgRegisterID->SrcCompHandlers[index]->Lock);
				}

				/* Remove our settings. */
				MsgRegisterID->SrcCompHandlers[index]->InUse          = GLOBUS_FALSE;
				MsgRegisterID->SrcCompHandlers[index]->MsgRecvFunc    = NULL;
				MsgRegisterID->SrcCompHandlers[index]->MsgRecvFuncArg = NULL;
			}
			globus_mutex_unlock(&MsgRegisterID->SrcCompHandlers[index]->Lock);
		}

		for (index = 0; index < MSG_COMP_ID_COUNT; index++)
		{
			if (MsgRegisterID->DstCompHandlers[index] == NULL)
				break;

			globus_mutex_lock(&MsgRegisterID->DstCompHandlers[index]->Lock);
			{
				/* Wait until it is not in use. */
				while (MsgRegisterID->DstCompHandlers[index]->CurrentCallers > 0)
				{
					globus_cond_wait(&MsgRegisterID->DstCompHandlers[index]->Cond,
					                 &MsgRegisterID->DstCompHandlers[index]->Lock);
				}

				/* Remove our settings. */
				MsgRegisterID->DstCompHandlers[index]->InUse          = GLOBUS_FALSE;
				MsgRegisterID->DstCompHandlers[index]->MsgRecvFunc    = NULL;
				MsgRegisterID->DstCompHandlers[index]->MsgRecvFuncArg = NULL;
			}
			globus_mutex_unlock(&MsgRegisterID->DstCompHandlers[index]->Lock);
		}
	}

	GlobusGFSHpssDebugExit();
}
               
typedef struct msg_async_callback_arg {
	msg_handler_t * MsgHandler;
	msg_comp_id_t   DstMsgCompID;
	msg_comp_id_t   SrcMsgCompID;
	int             MsgType;
	int             MsgLen;
	void          * Msg;
} msg_async_callback_arg_t;

static void
msg_send_msg_async_callback(void * UserArg)
{
	msg_async_callback_arg_t * callback_arg = (msg_async_callback_arg_t *) UserArg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Send the message. */
	callback_arg->MsgHandler->MsgRecvFunc(callback_arg->MsgHandler->MsgRecvFuncArg,
	                                      callback_arg->DstMsgCompID,
	                                      callback_arg->SrcMsgCompID,
	                                      callback_arg->MsgType,
	                                      callback_arg->MsgLen,
	                                      callback_arg->Msg);

	globus_mutex_lock(&callback_arg->MsgHandler->Lock);
	{
		/* Decrement the caller count. */
		callback_arg->MsgHandler->CurrentCallers--;

		/* Wake someone if needed. */
		if (callback_arg->MsgHandler->CurrentCallers == 0)
			globus_cond_signal(&callback_arg->MsgHandler->Cond);
	}
	globus_mutex_unlock(&callback_arg->MsgHandler->Lock);

	if (callback_arg->Msg != NULL)
		globus_free(callback_arg->Msg);
	globus_free(callback_arg);

	GlobusGFSHpssDebugExit();
}

static globus_result_t
msg_send_msg_async(msg_handle_t  * MsgHandle,
                   msg_handler_t * MsgHandler,
                   msg_comp_id_t   DstMsgCompID,
                   msg_comp_id_t   SrcMsgCompID,
                   int             MsgType,
                   int             MsgLen,
                   void          * Msg)
{
	globus_result_t            result       = GLOBUS_SUCCESS;
	msg_async_callback_arg_t * callback_arg = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&MsgHandler->Lock);
	{
		/* Make sure this handler entry is in use. */
		if (MsgHandler->InUse == GLOBUS_FALSE)
			goto unlock;

		/* Increase the use count. */
		MsgHandler->CurrentCallers++;

		/* Allocate the callback arg. */
		callback_arg = (msg_async_callback_arg_t *) globus_calloc(1, sizeof(msg_async_callback_arg_t));
		if (callback_arg == NULL)
		{
			result = GlobusGFSErrorMemory("msg_async_callback_arg_t");
			goto unlock;
		}

		callback_arg->MsgHandler   = MsgHandler;
		callback_arg->DstMsgCompID = DstMsgCompID;
		callback_arg->SrcMsgCompID = SrcMsgCompID;
		callback_arg->MsgType      = MsgType;
		callback_arg->MsgLen       = MsgLen;

		/* Copy the message. */
		if (MsgLen != 0)
		{
			callback_arg->Msg = globus_malloc(MsgLen);
			if (callback_arg->Msg == NULL)
			{
				result = GlobusGFSErrorMemory("msg_async_callback_arg_t->Msg");
				globus_free(callback_arg);
				goto unlock;
			}

			memcpy(callback_arg->Msg, Msg, MsgLen);
		}

		/* Register the callback. */
		result = globus_callback_register_oneshot(NULL,
		                                          NULL,
		                                          msg_send_msg_async_callback,
		                                          callback_arg);

		if (result != GLOBUS_SUCCESS)
			MsgHandler->CurrentCallers--;
	}
unlock:
	globus_mutex_unlock(&MsgHandler->Lock);

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

/*
 * Caller must give a valid SrcMsgCompID (not ANY or NONE). Message will
 * go to handler that:
 *  1) Specified SrcMsgCompID in SrcMsgCompIDs and DstMsgCompIDs is ANY
 *  2) Specified DstMsgCompID in DstMsgCompIDs
 *
 * Msg must not contain pointers unless it points to an external object.
 * msg_send() will duplicate Msg for each recipient.
 */
globus_result_t
msg_send(msg_handle_t * MsgHandle,
         msg_comp_id_t  DstMsgCompIDs,
         msg_comp_id_t  SrcMsgCompID,
         int            MsgType,
         int            MsgLen,
         void         * Msg)
{
	int             index1 = 0;
	int             index2 = 0;
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Validate our source component ID. */
	globus_assert(SrcMsgCompID != MSG_COMP_ID_NONE);
	globus_assert(SrcMsgCompID != MSG_COMP_ID_ANY);
	globus_assert(msg_count_comp_ids(SrcMsgCompID) == 1);

	/* Validate our destination component IDs. */
	globus_assert(DstMsgCompIDs != MSG_COMP_ID_NONE);
	if (DstMsgCompIDs != MSG_COMP_ID_ANY)
		globus_assert((DstMsgCompIDs & ~MSG_COMP_ID_MASK) == 0);

	if (DstMsgCompIDs == MSG_COMP_ID_ANY)
	{
		/* Get the index for the source component ID. */
		for (index1 = 0; index1 < MSG_COMP_ID_COUNT; index1++)
		{
			if ((SrcMsgCompID >> index1) == 1)
			{
				/* For each registered listener... */
				for (index2 = 0; index2 < MSG_COMP_ID_COUNT; index2++)
				{
					/* Send this message. */
					result = msg_send_msg_async(MsgHandle,
					                            &MsgHandle->SrcCompHandlers[index1][index2],
					                            DstMsgCompIDs,
					                            SrcMsgCompID,
					                            MsgType,
					                            MsgLen,
					                            Msg);

					if (result != GLOBUS_SUCCESS)
						break;
				}
				break;
			}
		}
	} else
	{
		/* Get the index for each destination component ID. */
		for (index1 = 0; index1 < MSG_COMP_ID_COUNT; index1++)
		{
			if ((1 << index1) & DstMsgCompIDs)
			{
				/* For each registered listener... */
				for (index2 = 0; index2 < MSG_COMP_ID_COUNT; index2++)
				{
					/* Send this message. */
					result = msg_send_msg_async(MsgHandle,
					                            &MsgHandle->DstCompHandlers[index1][index2],
					                            1 << index1, /* DstMsgCompID */
					                            SrcMsgCompID,
					                            MsgType,
					                            MsgLen,
					                            Msg);

					if (result != GLOBUS_SUCCESS)
						break;
				}
			}
		}
	}

	GlobusGFSHpssDebugExit();
	return result;
}


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

#define MSG_MAX_HANDLERS 16

struct msg_handle {
	globus_mutex_t Lock;
	globus_cond_t  Cond;

	struct {
		int               CurrentCallers;
		msg_recv_func_t   MsgRecvFunc;
		void            * CallbackArg;
	} Receivers[MSG_ID_MAX_ID];

	struct {
		globus_bool_t            InUse;
		int                      CurrentCallers;
		msg_recv_msg_type_func_t MsgFunc;
		void                   * CallbackArg;
	} MsgHandlers[MSG_TYPE_MAX][MSG_MAX_HANDLERS];
};


globus_result_t
msg_init(msg_handle_t ** MsgHandle)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the handle. */
	*MsgHandle = (msg_handle_t *) globus_calloc(1, sizeof(msg_handle_t));
	if (*MsgHandle == NULL)
	{
		result = GlobusGFSErrorMemory("msg_handle_t");
		goto cleanup;
	}

	globus_mutex_init(&(*MsgHandle)->Lock, NULL);
	globus_cond_init(&(*MsgHandle)->Cond, NULL);

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
msg_destroy(msg_handle_t * MsgHandle)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (MsgHandle != NULL)
	{
		globus_mutex_destroy(&MsgHandle->Lock);
		globus_cond_destroy(&MsgHandle->Cond);
		globus_free(MsgHandle);
	}

	GlobusGFSHpssDebugExit();
}

void
msg_register_recv(msg_handle_t    * MsgHandle,
                  msg_id_t          DestinationID,
                  msg_recv_func_t   MsgRecvFunc,
                  void            * CallbackArg)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Validate inputs. */
	globus_assert(DestinationID >= MSG_ID_MIN_ID);
	globus_assert(DestinationID <= MSG_ID_MAX_ID);

	globus_mutex_lock(&MsgHandle->Lock);
	{
		/* Make sure we aren't taking someone's spot. */
		globus_assert(MsgHandle->Receivers[DestinationID].MsgRecvFunc == NULL);

		/* Record this new routing. */
		MsgHandle->Receivers[DestinationID].MsgRecvFunc = MsgRecvFunc;
		MsgHandle->Receivers[DestinationID].CallbackArg = CallbackArg;
	}
	globus_mutex_unlock(&MsgHandle->Lock);

	GlobusGFSHpssDebugExit();
}

/*
 * We make use of CurrentCallers to make sure no one is using this
 * receiver before we remove it. We do this as a courtesy to our
 * users to that they know that once this function returns, there
 * is no possibility that some other module is still access them.
 * This will make their cleanup sane and simple.
 */
void
msg_unregister_recv(msg_handle_t * MsgHandle,
                    msg_id_t       DestinationID)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Validate inputs. */
	globus_assert(DestinationID >= MSG_ID_MIN_ID);
	globus_assert(DestinationID <= MSG_ID_MAX_ID);

	globus_mutex_lock(&MsgHandle->Lock);
	{
		/* Wait until we know no one is using this receiver. */
		while (MsgHandle->Receivers[DestinationID].CurrentCallers > 0)
		{
			globus_cond_wait(&MsgHandle->Cond, &MsgHandle->Lock);
		}

		/* Remove this old routing. */
		MsgHandle->Receivers[DestinationID].MsgRecvFunc = NULL;
		MsgHandle->Receivers[DestinationID].CallbackArg = NULL;
	}
	globus_mutex_unlock(&MsgHandle->Lock);

	GlobusGFSHpssDebugExit();
}

globus_result_t
msg_send(msg_handle_t * MsgHandle,
         int            NodeIndex,
         msg_id_t       DestinationID,
         msg_id_t       SourceID,
         int            MsgType,
         int            MsgLen,
         void         * Msg)
{
	msg_recv_func_t   msg_recv_func = NULL;
	void            * callback_arg  = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Validate inputs. */
	globus_assert(DestinationID >= MSG_ID_MIN_ID);
	globus_assert(DestinationID <= MSG_ID_MAX_ID);

	globus_mutex_lock(&MsgHandle->Lock);
	{
		if (MsgHandle->Receivers[DestinationID].MsgRecvFunc != NULL)
		{
			/*
			 * Save the function and arg for after we release the lock.
			 */
			msg_recv_func = MsgHandle->Receivers[DestinationID].MsgRecvFunc;
			callback_arg  = MsgHandle->Receivers[DestinationID].CallbackArg;

			/* Indicate that we are using this receiver. */
			MsgHandle->Receivers[DestinationID].CurrentCallers++;
		}
	}
	globus_mutex_unlock(&MsgHandle->Lock);

	if (msg_recv_func != NULL)
	{
		/* Send the message. */
		msg_recv_func(callback_arg,
		              NodeIndex,
		              DestinationID,
		              SourceID,
		              MsgType,
		              MsgLen,
		              Msg);

		globus_mutex_lock(&MsgHandle->Lock);
		{
			/* Indicate that we are done with this reciever. */
			MsgHandle->Receivers[DestinationID].CurrentCallers--;

			if (MsgHandle->Receivers[DestinationID].CurrentCallers == 0)
				globus_cond_signal(&MsgHandle->Cond);
		}
		globus_mutex_unlock(&MsgHandle->Lock);
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

msg_register_id_t
msg_register_for_type(msg_handle_t           * MsgHandle,
                      msg_type_t               MsgType,
                      msg_recv_msg_type_func_t MsgRecvMsgTypeFunc,
                      void                   * CallbackArg)
{
	int               index       = 0;
	msg_register_id_t register_id = -1;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&MsgHandle->Lock);
	{
		for (index = 0; index < MSG_MAX_HANDLERS; index++)
		{
			if (MsgHandle->MsgHandlers[MsgType][index].InUse == GLOBUS_FALSE)
			{
				/* Mark this as in use. */
				MsgHandle->MsgHandlers[MsgType][index].InUse = GLOBUS_TRUE;

				/* Record the caller info. */
				MsgHandle->MsgHandlers[MsgType][index].MsgFunc = MsgRecvMsgTypeFunc;
				MsgHandle->MsgHandlers[MsgType][index].CallbackArg = CallbackArg;

				/* Save the index as the register id. */
				register_id = index;

				break;
			}
		}
	}
	globus_mutex_unlock(&MsgHandle->Lock);

	globus_assert(register_id != -1);

	GlobusGFSHpssDebugExit();
	return register_id;
}

void
msg_unregister_for_type(msg_handle_t      * MsgHandle,
                        msg_type_t          MsgType,
                        msg_register_id_t   MsgRegisterID)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (MsgRegisterID == MSG_REGISTER_ID_NONE)
		goto cleanup;

	globus_mutex_lock(&MsgHandle->Lock);
	{
		while (MsgHandle->MsgHandlers[MsgType][MsgRegisterID].CurrentCallers > 0)
			globus_cond_wait(&MsgHandle->Cond, &MsgHandle->Lock);

		MsgHandle->MsgHandlers[MsgType][MsgRegisterID].InUse       = GLOBUS_FALSE;
		MsgHandle->MsgHandlers[MsgType][MsgRegisterID].MsgFunc     = NULL;
		MsgHandle->MsgHandlers[MsgType][MsgRegisterID].CallbackArg = NULL;
	}
	globus_mutex_unlock(&MsgHandle->Lock);

cleanup:
	GlobusGFSHpssDebugExit();
}

globus_result_t
msg_send_by_type(msg_handle_t * MsgHandle,
                 msg_type_t     MsgType,
                 int            MsgLen,
                 void         * Msg)
{
	int                        index        = 0;
	globus_result_t            result       = GLOBUS_SUCCESS;
	msg_recv_msg_type_func_t   msg_func     = NULL;
	void                     * callback_arg = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* For every message handler... */
	for (index = 0; index < MSG_MAX_HANDLERS; index++)
	{
		msg_func     = NULL;
		callback_arg = NULL;

		globus_mutex_lock(&MsgHandle->Lock);
		{
			if (MsgHandle->MsgHandlers[MsgType][index].InUse == GLOBUS_TRUE)
			{
				msg_func     = MsgHandle->MsgHandlers[MsgType][index].MsgFunc;
				callback_arg = MsgHandle->MsgHandlers[MsgType][index].CallbackArg;
				MsgHandle->MsgHandlers[MsgType][index].CurrentCallers++;
			}
		}
		globus_mutex_unlock(&MsgHandle->Lock);

		if (msg_func == NULL)
			continue;

		/* Now call it. */
		result = msg_func(callback_arg, MsgType, MsgLen, Msg);

		globus_mutex_lock(&MsgHandle->Lock);
		{
			MsgHandle->MsgHandlers[MsgType][index].CurrentCallers--;

			if (MsgHandle->MsgHandlers[MsgType][index].CurrentCallers == 0)
				globus_cond_signal(&MsgHandle->Cond);
		}
		globus_mutex_unlock(&MsgHandle->Lock);

		if (result != GLOBUS_SUCCESS)
			break;
	}

	GlobusGFSHpssDebugExit();
	return result;
}

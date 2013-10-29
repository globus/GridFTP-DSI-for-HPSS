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
#include <stdlib.h>

/*
 * Globus includes.
 */
#include <globus_gridftp_server.h>

/*
 * Local includes.
 */
#include "gridftp_dsi_hpss_buffer.h"
#include "gridftp_dsi_hpss_list.h"
#include "gridftp_dsi_hpss_misc.h"

#ifdef DMALLOC
/*
 * Dmalloc
 */
#include <dmalloc.h>
#endif /* DMALLOC */

#define BUFFER_ENTRY_LEN ((sizeof(buffer_entry_t)/8)*8 + (sizeof(buffer_entry_t)%8?8:0))
#define BUFFER_ALLOC_LEN(AllocSize) (AllocSize + BUFFER_ENTRY_LEN)
#define ENTRY_TO_BUFFER(entry) (((char *)entry) + BUFFER_ENTRY_LEN)
#define BUFFER_TO_ENTRY(buffer)((buffer_entry_t *)(buffer - BUFFER_ENTRY_LEN))

typedef enum {
	BUFFER_TYPE_FREE,
	BUFFER_TYPE_READY,
} buffer_type_t;

typedef struct {
	buffer_priv_id_t PrivateID;
	buffer_type_t    Type;
	globus_off_t     Offset;
	globus_off_t     Length;
	globus_bool_t    Flagged;
	globus_bool_t    InUse;
	globus_off_t     StoredOffset;
	globus_off_t     StoredLength;
} buffer_entry_t;

/*
 * Buffers are ordered on List by ID, Type, Offset.
 */
struct buffer_handle {
	globus_mutex_t   Lock;
	buffer_priv_id_t NextPrivateID;
	int              AllocSize;
	list_t         * List;
};

globus_result_t
buffer_init(int                BufferAllocSize,
            buffer_handle_t ** BufferHandle)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Allocate the handle. */
	*BufferHandle = (buffer_handle_t *) globus_calloc(1, sizeof(buffer_handle_t));
	if (*BufferHandle == NULL)
	{
		result = GlobusGFSErrorMemory("buffer_handle_t");
		goto cleanup;
	}

	/* Initialize the entries. */
	(*BufferHandle)->AllocSize = BufferAllocSize;
	globus_mutex_init(&(*BufferHandle)->Lock, NULL);

	/* Allocate the list. */
	result = list_init(&(*BufferHandle)->List);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

buffer_priv_id_t
buffer_create_private_list(buffer_handle_t * BufferHandle)
{
	buffer_priv_id_t id;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		id = BufferHandle->NextPrivateID++;
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
	return id;
}

void
buffer_destroy(buffer_handle_t * BufferHandle)
{
	buffer_entry_t * entry = NULL;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	if (BufferHandle != NULL)
	{
		/* Free the buffers. */
		while ((entry = list_remove_head(BufferHandle->List)) != NULL)
		{
			globus_free(entry);
		}

		/* Destroy the list. */
		list_destroy(BufferHandle->List);

		/* Destroy the handle. */
		globus_free(BufferHandle);
	}

	GlobusGFSHpssDebugExit();
}

int
buffer_get_alloc_size(buffer_handle_t * BufferHandle)
{
	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();
	GlobusGFSHpssDebugExit();
	return BufferHandle->AllocSize;
}

typedef struct {
	buffer_entry_t * ThisEntry;
	buffer_entry_t * NextEntry;
} search_order_arg_t;

static list_iterate_t
list_order_func(void * Data, void * CallbackArg)
{
	list_iterate_t       retval = LIST_ITERATE_CONTINUE;
	buffer_entry_t     * entry  = (buffer_entry_t *)Data;
	search_order_arg_t * search = (search_order_arg_t *) CallbackArg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* If this is not the same entry... */
	if (search->ThisEntry != entry)
	{
		/* Compare private IDs. */
		if (entry->PrivateID < search->ThisEntry->PrivateID)
		{
			/* Get next entry. */
			goto cleanup;
		}

		if (entry->PrivateID > search->ThisEntry->PrivateID)
		{
			/* This is our 'next' entry. */
			search->NextEntry = entry;

			/* Indicate full stop. */
			retval = LIST_ITERATE_DONE;

			/* Done. */
			goto cleanup;
		}

		/* Compare buffer types. */
		if (entry->Type < search->ThisEntry->Type)
		{
			/* Get next entry. */
			goto cleanup;
		}

		if (entry->Type > search->ThisEntry->Type)
		{
			/* This is our 'next' entry. */
			search->NextEntry = entry;

			/* Indicate full stop. */
			retval = LIST_ITERATE_DONE;

			/* Done. */
			goto cleanup;
		}

		/* Compare buffer offsets. */
		if (entry->Offset < search->ThisEntry->Offset)
		{
			/* Get next entry. */
			goto cleanup;
		}

		/*
		 * If it's > or =, this is our 'next' entry.
		 */
		search->NextEntry = entry;

		/* Indicate full stop. */
		retval = LIST_ITERATE_DONE;
	}

cleanup:
	GlobusGFSHpssDebugExit();
	return retval;
}

typedef enum {
	SEARCH_TYPE_FREE,
	SEARCH_TYPE_FLAGGED,
	SEARCH_TYPE_READY_AT_OFFSET,
	SEARCH_TYPE_NEXT_READY,
	SEARCH_TYPE_READY_COUNT,
} search_type_t;

typedef struct {
	search_type_t    Type;
	buffer_priv_id_t ID;
	globus_off_t     Offset;
	int              Count;
	buffer_entry_t * Entry;
} search_arg_t;

static list_iterate_t
list_search_func(void * Data, void * CallbackArg)
{
	list_iterate_t   retval = LIST_ITERATE_CONTINUE;
	buffer_entry_t * entry  = (buffer_entry_t *)Data;
	search_arg_t   * search = (search_arg_t *) CallbackArg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	search->Entry = NULL;

	/* Always enforce private IDs. */
	if (search->ID != entry->PrivateID)
		goto cleanup;

	/* Skip inuse buffers unless we are looking for flagged buffers. */
	if (entry->InUse == GLOBUS_TRUE && search->Type != SEARCH_TYPE_FLAGGED)
		goto cleanup;

	switch (search->Type)
	{
	case SEARCH_TYPE_FREE:
		if (entry->Type == BUFFER_TYPE_FREE)
		{
			/* We have our match. */
			search->Entry = entry;
			/* Indicate that we are done. */
			retval = LIST_ITERATE_DONE;
		}
		break;

	case SEARCH_TYPE_FLAGGED:
		if (entry->Flagged == GLOBUS_TRUE)
		{
			/* We have our match. */
			search->Entry = entry;
			/* Indicate that we are done. */
			retval = LIST_ITERATE_DONE;
		}
		break;

	case SEARCH_TYPE_READY_AT_OFFSET:
		if (entry->Type == BUFFER_TYPE_READY && entry->Offset == search->Offset)
		{
			/* We have our match. */
			search->Entry = entry;
			/* Indicate that we are done. */
			retval = LIST_ITERATE_DONE;
		}
		break;

	case SEARCH_TYPE_NEXT_READY:
		if (entry->Type == BUFFER_TYPE_READY)
		{
			/* We have our match. */
			search->Entry = entry;
			/* Indicate that we are done. */
			retval = LIST_ITERATE_DONE;
		}
		break;

	case SEARCH_TYPE_READY_COUNT:
		if (entry->Type == BUFFER_TYPE_READY)
		{
			/* We have our match. */
			search->Count++;
		}
		break;
	}

cleanup:
	GlobusGFSHpssDebugExit();
	return retval;
}

globus_result_t
buffer_allocate_buffer(buffer_handle_t *  BufferHandle,
                       buffer_priv_id_t   PrivateID,
                       char            ** Buffer,
                       globus_off_t    *  Length)
{
	globus_result_t    result = GLOBUS_SUCCESS;
	buffer_entry_t   * entry  = NULL;
	search_order_arg_t order_arg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*Buffer = NULL;

	/* Allocate a new buffer. */
	entry = (buffer_entry_t *) globus_malloc(BUFFER_ALLOC_LEN(BufferHandle->AllocSize));
	if (entry == NULL)
	{
		result = GlobusGFSErrorMemory("buffer_entry_t");
		goto cleanup;
	}

	/* Initialize it. */
	entry->PrivateID    = PrivateID;
	entry->Type         = BUFFER_TYPE_FREE;
	entry->Offset       = (globus_off_t)-1;
	entry->Length       = BufferHandle->AllocSize;
	entry->Flagged      = GLOBUS_FALSE;
	entry->InUse        = GLOBUS_TRUE;
	entry->StoredOffset = (globus_off_t)-1;
	entry->StoredLength = (globus_off_t)-1;

	globus_mutex_lock(&BufferHandle->Lock);
	{
		/* Determine where it goes. */
		memset(&order_arg, 0, sizeof(search_order_arg_t));
		order_arg.ThisEntry = entry;

		/* Find the next item. */
		list_iterate(BufferHandle->List, list_order_func, &order_arg);

		if (order_arg.NextEntry == NULL)
			result = list_insert_at_end(BufferHandle->List, entry);
		else
			result = list_insert_before(BufferHandle->List, entry, order_arg.NextEntry);

		if (result != GLOBUS_SUCCESS)
			goto unlock;

		*Buffer = ENTRY_TO_BUFFER(entry);
		*Length = BufferHandle->AllocSize;
	}
unlock:
	globus_mutex_unlock(&BufferHandle->Lock);

cleanup:
	if (result != GLOBUS_SUCCESS)
	{
		if (entry != NULL)
			globus_free(entry);

		GlobusGFSHpssDebugExitWithError();
		return result;
	}

	GlobusGFSHpssDebugExit();
	return GLOBUS_SUCCESS;
}

void
buffer_get_free_buffer(buffer_handle_t *  BufferHandle,
                       buffer_priv_id_t   PrivateID,
                       char            ** Buffer,
                       globus_off_t    *  Length)
{
	search_arg_t search_arg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*Buffer = NULL;

	memset(&search_arg, 0, sizeof(search_arg_t));
	search_arg.Type      = SEARCH_TYPE_FREE;
	search_arg.ID        = PrivateID;

	globus_mutex_lock(&BufferHandle->Lock);
	{
		list_iterate(BufferHandle->List, list_search_func, &search_arg);

		if (search_arg.Entry != NULL)
		{
			/* Indicate that this buffer is in use. */
			search_arg.Entry->InUse = GLOBUS_TRUE;

			/* Save the return values. */
			*Buffer = ENTRY_TO_BUFFER(search_arg.Entry);
			*Length = search_arg.Entry->Length;
		}
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
}

/* Mark not inuse. */
void
buffer_store_free_buffer(buffer_handle_t  * BufferHandle,
                         buffer_priv_id_t   PrivateID,
                         char             * Buffer)
{
	buffer_entry_t   * entry = BUFFER_TO_ENTRY(Buffer);
	search_order_arg_t order_arg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		/* Update the buffer entry. */
		entry->Type      = BUFFER_TYPE_FREE;
		entry->PrivateID = PrivateID;
		entry->Offset    = (globus_off_t)-1;
		entry->Length    = BufferHandle->AllocSize;
		entry->InUse     = GLOBUS_FALSE;

		/* Determine where it goes. */
		memset(&order_arg, 0, sizeof(search_order_arg_t));
		order_arg.ThisEntry = entry;

		/* Find the next item. */
		list_iterate(BufferHandle->List, list_order_func, &order_arg);

		if (order_arg.NextEntry != NULL)
			list_move_before(BufferHandle->List, entry, order_arg.NextEntry);
		else
			list_move_to_end(BufferHandle->List, entry);
	}
	globus_mutex_unlock(&BufferHandle->Lock);
}

/* Mark inuse. */
void
buffer_get_ready_buffer(buffer_handle_t  *  BufferHandle,
                        buffer_priv_id_t    PrivateID,
                        char             ** Buffer,
                        globus_off_t        Offset,
                        globus_off_t     *  Length)
{
	search_arg_t search_arg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	/* Initialize the return value. */
	*Buffer = NULL;

	memset(&search_arg, 0, sizeof(search_arg_t));
	search_arg.Type      = SEARCH_TYPE_READY_AT_OFFSET;
	search_arg.ID        = PrivateID;
	search_arg.Offset    = Offset;

	globus_mutex_lock(&BufferHandle->Lock);
	{
		list_iterate(BufferHandle->List, list_search_func, &search_arg);

		if (search_arg.Entry != NULL)
		{
			/* Indicate that this buffer is in use. */
			search_arg.Entry->InUse = GLOBUS_TRUE;

			/* Save the return values. */
			*Buffer = ENTRY_TO_BUFFER(search_arg.Entry);
			*Length = search_arg.Entry->Length;
		}
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
}

/* Mark not inuse. */
void
buffer_store_ready_buffer(buffer_handle_t  * BufferHandle,
                          buffer_priv_id_t   PrivateID,
                          char             * Buffer,
                          globus_off_t       Offset,
                          globus_off_t       Length)
{
	buffer_entry_t   * entry = BUFFER_TO_ENTRY(Buffer);
	search_order_arg_t order_arg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		entry->Type      = BUFFER_TYPE_READY;
		entry->PrivateID = PrivateID;
		entry->Offset    = Offset;
		entry->Length    = Length;
		entry->InUse     = GLOBUS_FALSE;

		/* Determine where it goes. */
		memset(&order_arg, 0, sizeof(search_order_arg_t));
		order_arg.ThisEntry = entry;

		/* Find the next item. */
		list_iterate(BufferHandle->List, list_order_func, &order_arg);

		if (order_arg.NextEntry != NULL)
			list_move_before(BufferHandle->List, entry, order_arg.NextEntry);
		else
			list_move_to_end(BufferHandle->List, entry);
	}
	globus_mutex_unlock(&BufferHandle->Lock);
}

void
buffer_set_buffer_ready(buffer_handle_t  * BufferHandle,
                        buffer_priv_id_t   PrivateID,
                        char             * Buffer,
                        globus_off_t       Offset,
                        globus_off_t       Length)
{
	buffer_entry_t   * entry = BUFFER_TO_ENTRY(Buffer);
	search_order_arg_t order_arg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		entry->Type      = BUFFER_TYPE_READY;
		entry->PrivateID = PrivateID;
		entry->Offset    = Offset;
		entry->Length    = Length;

		/* Determine where it goes. */
		memset(&order_arg, 0, sizeof(search_order_arg_t));
		order_arg.ThisEntry = entry;

		/* Find the next item. */
		list_iterate(BufferHandle->List, list_order_func, &order_arg);

		if (order_arg.NextEntry != NULL)
			list_move_before(BufferHandle->List, entry, order_arg.NextEntry);
		else
			list_move_to_end(BufferHandle->List, entry);
	}
	globus_mutex_unlock(&BufferHandle->Lock);
}

void
buffer_set_buffer_free(buffer_handle_t  * BufferHandle,
                       buffer_priv_id_t   PrivateID,
                       char             * Buffer,
                       globus_off_t     * Length)
{
	buffer_entry_t   * entry = BUFFER_TO_ENTRY(Buffer);
	search_order_arg_t order_arg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		entry->Type      = BUFFER_TYPE_FREE;
		entry->PrivateID = PrivateID;
		entry->Length    = BufferHandle->AllocSize;
		entry->Offset    = (globus_off_t)-1;
		*Length          = BufferHandle->AllocSize;

		/* Determine where it goes. */
		memset(&order_arg, 0, sizeof(search_order_arg_t));
		order_arg.ThisEntry = entry;

		/* Find the next item. */
		list_iterate(BufferHandle->List, list_order_func, &order_arg);

		if (order_arg.NextEntry != NULL)
			list_move_before(BufferHandle->List, entry, order_arg.NextEntry);
		else
			list_move_to_end(BufferHandle->List, entry);
	}
	globus_mutex_unlock(&BufferHandle->Lock);


	GlobusGFSHpssDebugExit();
}

void
buffer_set_private_id(buffer_handle_t  * BufferHandle,
                      buffer_priv_id_t   PrivateID,
                      char             * Buffer)
{
	buffer_entry_t   * entry = BUFFER_TO_ENTRY(Buffer);
	search_order_arg_t order_arg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		entry->PrivateID = PrivateID;

		/* Determine where it goes. */
		memset(&order_arg, 0, sizeof(search_order_arg_t));
		order_arg.ThisEntry = entry;

		/* Find the next item. */
		list_iterate(BufferHandle->List, list_order_func, &order_arg);

		if (order_arg.NextEntry != NULL)
			list_move_before(BufferHandle->List, entry, order_arg.NextEntry);
		else
			list_move_to_end(BufferHandle->List, entry);
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
}

void
buffer_flag_buffer(buffer_handle_t  * BufferHandle,
                   buffer_priv_id_t   PrivateID,
                   char             * Buffer)
{
	buffer_entry_t * entry = BUFFER_TO_ENTRY(Buffer);

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		entry->Flagged    = GLOBUS_TRUE;
		entry->PrivateID = PrivateID;
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
}

void
buffer_get_flagged_buffer(buffer_handle_t  *  BufferHandle,
                          buffer_priv_id_t    PrivateID,
                          char             ** Buffer,
                          globus_off_t     *  Offset,
                          globus_off_t     *  Length)
{
	search_arg_t search_arg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	memset(&search_arg, 0, sizeof(search_arg_t));
	search_arg.Type = SEARCH_TYPE_FLAGGED;
	search_arg.ID   = PrivateID;

	globus_mutex_lock(&BufferHandle->Lock);
	{
		list_iterate(BufferHandle->List, list_search_func, &search_arg);

		if (search_arg.Entry != NULL)
		{
			/* Indicate that this buffer is in use. */
			search_arg.Entry->InUse = GLOBUS_TRUE;

			/* Save the return values. */
			*Buffer = ENTRY_TO_BUFFER(search_arg.Entry);
			*Length = search_arg.Entry->Length;
			*Offset = search_arg.Entry->Offset;
		}
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
}

void
buffer_clear_flag(buffer_handle_t  * BufferHandle,
                  buffer_priv_id_t   PrivateID,
                  char             * Buffer)
{
	buffer_entry_t * entry = BUFFER_TO_ENTRY(Buffer);

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		entry->Flagged   = GLOBUS_FALSE;
		entry->PrivateID = PrivateID;
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
}

int
buffer_get_ready_buffer_count(buffer_handle_t  * BufferHandle,
                              buffer_priv_id_t   PrivateID)
{
	search_arg_t search_arg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	memset(&search_arg, 0, sizeof(search_arg_t));
	search_arg.Type      = SEARCH_TYPE_READY_COUNT;
	search_arg.ID        = PrivateID;

	globus_mutex_lock(&BufferHandle->Lock);
	{
		list_iterate(BufferHandle->List, list_search_func, &search_arg);
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
	return search_arg.Count;
}

void
buffer_set_offset_length(buffer_handle_t  * BufferHandle,
                         buffer_priv_id_t   PrivateID,
                         char             * Buffer,
                         globus_off_t       Offset,
                         globus_off_t       Length)
{
	buffer_entry_t   * entry = BUFFER_TO_ENTRY(Buffer);
	search_order_arg_t order_arg;

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		entry->Offset = Offset;
		entry->Length = Length;

		/* Determine where it goes. */
		memset(&order_arg, 0, sizeof(search_order_arg_t));
		order_arg.ThisEntry = entry;

		/* Find the next item. */
		list_iterate(BufferHandle->List, list_order_func, &order_arg);

		if (order_arg.NextEntry != NULL)
			list_move_before(BufferHandle->List, entry, order_arg.NextEntry);
		else
			list_move_to_end(BufferHandle->List, entry);
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
}

void
buffer_store_offset_length(buffer_handle_t  * BufferHandle,
                           buffer_priv_id_t   PrivateID,
                           char             * Buffer,
                           globus_off_t       Offset,
                           globus_off_t       Length)
{
	buffer_entry_t * entry = BUFFER_TO_ENTRY(Buffer);

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		entry->StoredOffset = Offset;
		entry->StoredLength = Length;
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
}

void
buffer_get_stored_offset_length(buffer_handle_t  * BufferHandle,
                                buffer_priv_id_t   PrivateID,
                                char             * Buffer,
                                globus_off_t     * Offset,
                                globus_off_t     * Length)
{
	buffer_entry_t * entry = BUFFER_TO_ENTRY(Buffer);

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		*Offset = entry->StoredOffset;
		*Length = entry->StoredLength;
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
}

void
buffer_clear_stored_offset_length(buffer_handle_t  * BufferHandle,
                                  buffer_priv_id_t   PrivateID,
                                  char             * Buffer)
{
	buffer_entry_t * entry = BUFFER_TO_ENTRY(Buffer);

	GlobusGFSName(__func__);
	GlobusGFSHpssDebugEnter();

	globus_mutex_lock(&BufferHandle->Lock);
	{
		entry->StoredOffset = (globus_off_t)-1;
		entry->StoredLength = (globus_off_t)-1;
	}
	globus_mutex_unlock(&BufferHandle->Lock);

	GlobusGFSHpssDebugExit();
}

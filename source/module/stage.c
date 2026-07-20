/*
 * System includes
 */
#include <sys/select.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/*
 * Local includes
 */
#include "hpss_log.h"
#include "logging.h"
#include "stage.h"
#include "utils.h"
#include "hpss.h"

/*
 *
 * The staging code in this DSI has gone through several iterations over the years
 * to adjust as admins of different HPSS configurations find issues at their sites.
 * Use this space to record the detailed history of staging with this DSI so that
 * we know why we are heading in the direction we are heading.
 *
 * BACKGROUND: The Globus Transfer service drives the staging requests when a
 * retrieve task is submitted for an HPSS endpoint/collection. The Transfer service
 * connects via GridFTP and issues N (<=64) stage requests. Transfer's expectations
 * on the response to the stage request are:
 *
 * - respond that the file is on disk
 * - respond that the file is on a tape-only cos
 * - respond that the file is being retrieved from archive
 *
 * The Transfer service does not keep any state about the staging process other than
 * which files are reported as on-disk or tape-only. When one or more of the N files
 * are known to be on disk, the staging GridFTP connection is terminated and a new 
 * GridFTP connection is created for transferring the on-disk files. During those
 * transfers, the remaining M files will continue to stage. Once the transfers are
 * complete, the GridFTP transfer connection terminates, a new GridFTP staging
 * session begins with N files; Transfer will backfill the stage queue with new
 * files to replace those that have transferred successfully.
 *
 * Transfer uses the stage command by polling; it will continue to issue the command
 * for a file until it becomes resident. It is the DSI's responsibility to make sure
 * the re-issue of stage commands does not cause any problems.
 *
 * In our first attempt (very early on, possibly pre-release), the DSI used
 * hpss_Open(O_NONBLOCK) and hpss_Stage(BFS_ASYNCH_CALL). However, that caused us to
 * block in hpss_Close(). Given the transient use of the GridFTP staging process by
 * Transfer, this was a real issue; the GridFTP processes could not exit until every
 * file had been staged.
 *
 * So we used hpss_StageCallBack() to avoid blocking. We would (1) check file
 * residency and (2) issue hpss_StageCallBack() if the file was not on disk. This caused
 * two new issues:
 *
 * 1) We did not supply a callback address to hpss_StageCallBack() because the GridFTP
 * process is transient and we wouldn't be around to receive it. This caused HPSS
 * monitors to go red-ball-of-doom. So we provided a hack to blackhole the callback
 * requests using 'nc' run from outside of the DSI.
 *
 * 2) The reuse of hpss_StageCallBack() caused the core server to run out of threads;
 * every stage request, even for the same file, was occupying a new thread. HPSS patched
 * the core server to consolidate threads to reduce thread count (or something to that
 * extent).
 *
 * Life was good, however, a HPSS site admin noticed that even though the core server
 * thread count was under control, RTMU (real time monitor) had a ridiculous number of
 * entries for the same file. This made the RTMU unmanageable. So in order to avoid
 * duplicate stage requests, the DSI started to use hpss_GetAsyncStatus() which allows us
 * to check if a stage request already exists for the tuple (request ID, bitfile ID).
 * Since the GridFTP process is transient and Transfer does not keep state, the 
 * request ID had to be predictable so I tested with a hardcoded request ID and it seemed
 * to do the trick; admins were happy that RTMU was no longer overloaded with requests.
 *
 * But there is always some catch. NERSC reported in Version 2.16 on HPSS 7.4.3 that it
 * would take hours before the first stage request would show up in RTMU for a given task.
 * Looking into the debug logs, turns out that (at least on 7.4.3), the constant request
 * ID was gating stage requests between _all_ current HPSS transfer retrieve tasks. So
 * their endpoint was only staging 1-3 files at a time across _all_ tasks.
 *
 * Since the Transfer service does not keep state, we need a way to produce independent
 * but predictable request IDs per file per task in order to avoid gating stage requests
 * and to give the DSI a way to query for existing stage requests for a (task, file) tuple.
 * This version of staging does that by computing a request ID based on information known
 * to the DSI at stage time:
 *
 * - The task ID is a UUID supplied by Transfer and is unique to this user's task
 * - The bitfile's ID which is unique to this file.
 *
 * We xor specific contents of the bitfile ID with the Transfer task ID to produce a
 * seemingly-random-but-predictable request ID that is hopefully unique to this
 * (task,file) tuple.
 *
 * For what it's worth, there is a design (pending funding) to improve this which includes
 * having Transfer keep some state between requests.
 */

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) || HPSS_MAJOR_VERSION >= 8
#define bitfile_id_t bfs_bitfile_obj_handle_t
#define ATTR_TO_BFID(x) (x.Attrs.BitfileObj.BfId)
#else
#define bitfile_id_t hpssoid_t
#define ATTR_TO_BFID(x) (x.Attrs.BitfileId)
#endif


/*
 * Fallback to a constant request ID for stage requests when a task ID is not
 * avilable so that we can query the stage request status between processes.
 */
#if HPSS_MAJOR_VERSION >= 8
static hpss_reqid_t DEFAULT_REQUEST_ID = {0xdeadbeef, 0xdead, 0xbeef, 0xde, 0xad, {0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef}};
#else
static hpss_reqid_t DEFAULT_REQUEST_ID = 0xDEADBEEF;
#endif

static void
_bitfile_id_to_bytes(bitfile_id_t * BitfileID, unsigned char Bytes[UUID_BYTE_COUNT])
{
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) || HPSS_MAJOR_VERSION >= 8
    memcpy(Bytes, BitfileID->BfId.Bytes, UUID_BYTE_COUNT);
#else
    hpss_uuid_to_bytes(&BitfileID->ObjectID, Bytes);
#endif
}

static void
_bytes_to_request_id(const unsigned char Bytes[UUID_BYTE_COUNT], hpss_reqid_t * RequestID)
{
#if HPSS_MAJOR_VERSION >= 8
    // Convert to a UUID
    bytes_to_hpss_uuid(Bytes, RequestID);
#else
    // Convert to unsigned
    bytes_to_unsigned(Bytes, RequestID);
#endif
}

static void
_generate_request_id(
    const char                  *  TaskID,
    bitfile_id_t                *  BitfileID,
    hpss_reqid_t                *  RequestID)
{
    // If we do not have a Task ID, log a warning and return the default.
    if (!is_valid_uuid(TaskID))
    {
        WARN("No task ID available for stage request. Using the default request id.");
        memcpy(RequestID, &DEFAULT_REQUEST_ID, sizeof(*RequestID));
        return;
    }

    // Convert Task ID to a bytes array
    unsigned char task_id_bytes[UUID_BYTE_COUNT];
    uuid_str_to_bytes(TaskID, task_id_bytes);

    // Convert BitfileID to a byte array
    unsigned char bitfile_id_bytes[UUID_BYTE_COUNT];
    _bitfile_id_to_bytes(BitfileID, bitfile_id_bytes);

    // Combine the two byte arrays
    unsigned char request_id_bytes[UUID_BYTE_COUNT];
    for (int i = 0; i < UUID_BYTE_COUNT; i++)
    {
        request_id_bytes[i] = task_id_bytes[i] ^ bitfile_id_bytes[i];
    }

    // Convert our bytes array into a request ID.
    _bytes_to_request_id(request_id_bytes, RequestID);

    // This debug is here to allow us to verify the computation
    DEBUG("Using request ID %s for Task ID %s and %s %s",
        HPSS_REQID_T(*RequestID),
        TaskID,
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) || HPSS_MAJOR_VERSION >= 8
        "bitfile ID",
        HPSSOID_T(BitfileID->BfId)
#else
        "bitfile's ObjectID",
        HPSS_UUID_T(BitfileID->ObjectID)
#endif
    );
}

static globus_result_t
check_request_status(hpss_reqid_t RequestID, bitfile_id_t *BitfileID, int *Status)
{
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) || HPSS_MAJOR_VERSION >= 8
    int retval = Hpss_GetAsyncStatus(RequestID, BitfileID, Status);
#else
    int retval = Hpss_GetAsynchStatus(RequestID, BitfileID, Status);
#endif

    if (retval)
        return hpss_error_to_globus_result(retval);

    switch (*Status)
    {
    case HPSS_STAGE_STATUS_UNKNOWN:
        WARN("Stage request status UNKNOWN");
        break;
    case HPSS_STAGE_STATUS_ACTIVE:
        DEBUG("Stage request status ACTIVE");
        break;
    case HPSS_STAGE_STATUS_QUEUED:
        DEBUG("Stage request status QUEUED");
        break;
    }

    return GLOBUS_SUCCESS;
}

int
min(size_t x, size_t y)
{
    if (x < y)
        return x;
    return y;
}

static globus_result_t
_build_callback_addr(
    hpss_reqid_t                   CallbackID,
    bfs_callback_addr_t         *  CallbackAddr) // OUT
{
    memset(CallbackAddr, 0, sizeof(*CallbackAddr));

    const char *callback_addr_str = getenv("ASYNC_CALLBACK_ADDR");
    if (callback_addr_str)
    {
        char node[NI_MAXHOST];
        char serv[NI_MAXSERV];

        memset(node, 0, sizeof(node));
        memset(serv, 0, sizeof(serv));

        char *colon;
        if ((colon = strchr(callback_addr_str, ':')))
        {
            strncpy(node,
                    callback_addr_str,
                    min(sizeof(node) - 1, colon - callback_addr_str));
            strncpy(serv, colon + 1, sizeof(serv));
        } else
        {
            strncpy(node, callback_addr_str, sizeof(node) - 1);
        }

        char errbuf[HPSS_NET_MAXBUF];
        int retval = Hpss_net_getaddrinfo(node,
                                          serv,
                                          0,
                                          HPSS_IPPROTO_TCP,
                                          &CallbackAddr->sockaddr,
                                          errbuf,
                                          sizeof(errbuf));

        if (retval)
        {
            ERROR("Failed to set stage callback address %s: %d (%s) - %s",
                   callback_addr_str,
                   retval,
                   gai_strerror(hpss_error_status(retval)),
                   errbuf);
            return hpss_error_to_globus_result(retval);
        }
    }

    CallbackAddr->id = CallbackID;
    return GLOBUS_SUCCESS;
}


static globus_result_t
submit_stage_request(const char *Pathname, hpss_reqid_t RequestID)
{
    hpss_fileattr_t fattrs;
    int retval = Hpss_FileGetAttributes((char *)Pathname, &fattrs);
    if (retval)
        return hpss_error_to_globus_result(retval);

    bfs_callback_addr_t callback_addr;

    globus_result_t result = _build_callback_addr(RequestID, &callback_addr);
    if (result)
        return result;

    DEBUG("Requesting stage for %s", Pathname);

    /*
     * We use hpss_StageCallBack() so that we do not block while the
     * stage completes. We could use hpss_Open(O_NONBLOCK) and then
     * hpss_Stage(BFS_ASYNCH_CALL) but then we block in hpss_Close().
     */
    bitfile_id_t bitfile_id;
    retval = Hpss_StageCallBack((char *)Pathname,
                                cast64m(0),
                                fattrs.Attrs.DataLength,
                                0,
                                &callback_addr,
                                BFS_STAGE_ALL,
                                &RequestID,
                                &bitfile_id);
    if (retval)
        return hpss_error_to_globus_result(retval);

    return GLOBUS_SUCCESS;
}

static residency_t
check_xattr_residency(hpss_xfileattr_t *XFileAttr)
{
    /* Check if this is a regular file. */
    if (XFileAttr->Attrs.Type != NS_OBJECT_TYPE_FILE &&
        XFileAttr->Attrs.Type != NS_OBJECT_TYPE_HARD_LINK)
    {
        return RESIDENCY_RESIDENT;
    }

    /* Check if the top level is tape. */
    if (XFileAttr->SCAttrib[0].Flags & BFS_BFATTRS_LEVEL_IS_TAPE)
    {
        return RESIDENCY_TAPE_ONLY;
    }

    /* Handle zero length files. */
    if (eqz64m(XFileAttr->Attrs.DataLength))
    {
        return RESIDENCY_RESIDENT;
    }

    /*
     * Determine the archive status. Due to holes, we can not expect
     * XFileAttr->Attrs.DataLength bytes on disk. And we really don't know
     * how much data is really in this file. So the algorithm assumes the
     * file is staged unless it finds a tape SC that has more BytesAtLevel
     * than the disk SCs before it.
     */
    int      level     = 0;
    uint64_t max_bytes = 0;

    for (level = 0; level < HPSS_MAX_STORAGE_LEVELS; level++)
    {
        if (XFileAttr->SCAttrib[level].Flags & BFS_BFATTRS_LEVEL_IS_DISK)
        {
            /* Save the largest count of bytes on disk. */
            if (gt64(XFileAttr->SCAttrib[level].BytesAtLevel, max_bytes))
                max_bytes = XFileAttr->SCAttrib[level].BytesAtLevel;
        } else if (XFileAttr->SCAttrib[level].Flags & BFS_BFATTRS_LEVEL_IS_TAPE)
        {
            /* File is purged if more bytes are on tape. */
            if (gt64(XFileAttr->SCAttrib[level].BytesAtLevel, max_bytes))
                return RESIDENCY_ARCHIVED;
        }
    }

    return RESIDENCY_RESIDENT;
}

static void
free_xfileattr(hpss_xfileattr_t *XFileAttr)
{
    int level    = 0;
    int vv_index = 0;

    /* Free the extended information. */
    for (level = 0; level < HPSS_MAX_STORAGE_LEVELS; level++)
    {
        for (vv_index = 0; vv_index < XFileAttr->SCAttrib[level].NumberOfVVs;
             vv_index++)
        {
            if (XFileAttr->SCAttrib[level].VVAttrib[vv_index].PVList != NULL)
            {
                free(XFileAttr->SCAttrib[level].VVAttrib[vv_index].PVList);
            }
        }
    }
}

static globus_result_t
check_file_residency(const char *Pathname, residency_t *Residency)
{
    int              retval = 0;
    hpss_xfileattr_t xattr;

    memset(&xattr, 0, sizeof(hpss_xfileattr_t));

    /*
     * Stat the object. Without API_GET_XATTRS_NO_BLOCK, this call would hang
     * on any file moving between levels in its hierarchy (ie staging).
     */
    retval = Hpss_FileGetXAttributes((char *)Pathname,
                                     API_GET_STATS_FOR_ALL_LEVELS |
                                         API_GET_XATTRS_NO_BLOCK,
                                     0,
                                     &xattr);

    if (retval)
        return hpss_error_to_globus_result(retval);

    *Residency = check_xattr_residency(&xattr);

    /* Release the hpss_xfileattr_t */
    free_xfileattr(&xattr);

    switch (*Residency)
    {
    case RESIDENCY_ARCHIVED:
        DEBUG("File is ARCHIVED: %s", Pathname);
        break;
    case RESIDENCY_RESIDENT:
        DEBUG("File is RESIDENT: %s", Pathname);
        break;
    case RESIDENCY_TAPE_ONLY:
        DEBUG("File is TAPE_ONLY: %s", Pathname);
        break;
    }

    return GLOBUS_SUCCESS;
}

static globus_result_t
stage_get_timeout(globus_gfs_operation_t     Operation,
                  globus_gfs_command_info_t *CommandInfo,
                  int *                      Timeout)
{
    globus_result_t result;
    char **         argv   = NULL;
    int             argc   = 0;
    int             retval = 0;

    /* Get the command arguments. */
    result = globus_gridftp_server_query_op_info(Operation,
                                                 CommandInfo->op_info,
                                                 GLOBUS_GFS_OP_INFO_CMD_ARGS,
                                                 &argv,
                                                 &argc);

    if (result)
        return GlobusGFSErrorWrapFailed("Unable to get command args", result);

    /* Convert the timeout. */
    retval = sscanf(argv[2], "%d", Timeout);
    if (retval != 1)
        return GlobusGFSErrorGeneric("Illegal timeout value");

    return GLOBUS_SUCCESS;
}

static globus_result_t
get_bitfile_id(const char *Pathname, bitfile_id_t *bitfile_id)
{
    hpss_fileattr_t attrs;
    int retval = Hpss_FileGetAttributes((char *)Pathname, &attrs);
    if (retval)
        return hpss_error_to_globus_result(retval);

    memcpy(bitfile_id, &ATTR_TO_BFID(attrs), sizeof(bitfile_id_t));
    return GLOBUS_SUCCESS;
}

static char *
generate_output(const char *Pathname, residency_t Residency)
{
    if (Residency == RESIDENCY_RESIDENT)
        return globus_common_create_string(
            "250 Stage of file %s succeeded.\r\n", Pathname);

    if (Residency == RESIDENCY_TAPE_ONLY)
        return globus_common_create_string(
            "250 %s is on a tape only class of service.\r\n", Pathname);

    return globus_common_create_string(
        "450 %s: is being retrieved from the archive...\r\n", Pathname);
}

/* Returns 1 if Timeout has elapsed, 0 otherwise. */
static int
pause_1_second(time_t StartTime, int Timeout)
{
    if ((time(NULL) - StartTime) > Timeout)
        return 1;

    // Sleep for 1.0 seconds
    struct timeval tv;
    tv.tv_sec  = 1;
    tv.tv_usec = 0;
    select(0, NULL, NULL, NULL, &tv);

    return 0;
}

// Utils entry point
globus_result_t
stage_ex(
    const char   * Path,
    int            Timeout,
    const char   * TaskID,
    hpss_reqid_t * RequestID,
    residency_t  * Residency)
{
    int             time_elapsed = 0;
    time_t          start_time   = time(NULL);
    bitfile_id_t    bitfile_id;
    globus_result_t result;

    *Residency = RESIDENCY_ARCHIVED;

    result = get_bitfile_id(Path, &bitfile_id);
    if (result)
        goto cleanup;

    while (*Residency == RESIDENCY_ARCHIVED && !time_elapsed)
    {
        result = check_file_residency(Path, Residency);
        if (result)
            goto cleanup;
        if (*Residency != RESIDENCY_ARCHIVED)
            break;

        // Generate request ID
        _generate_request_id(TaskID, &bitfile_id, RequestID);

        int status;
        result = check_request_status(*RequestID, &bitfile_id, &status);
        if (result)
            goto cleanup;

        if (status == HPSS_STAGE_STATUS_UNKNOWN)
        {
            result = submit_stage_request(Path, *RequestID);
            if (result)
                goto cleanup;
        }

        time_elapsed = pause_1_second(start_time, Timeout);
    }

cleanup:
    return result;
}

// DSI entry point
void
stage(globus_gfs_operation_t     Operation,
      globus_gfs_command_info_t *CommandInfo,
      commands_callback          Callback)
{
    int             timeout;
    char *          command_output = NULL;
    globus_result_t result;

    result = stage_get_timeout(Operation, CommandInfo, &timeout);
    if (result)
        goto cleanup;

    char * task_id = NULL;
    globus_gridftp_server_get_task_id(Operation, &task_id);

    hpss_reqid_t request_id;
    residency_t residency;
    result = stage_ex(CommandInfo->pathname, timeout, task_id, &request_id, &residency);
    if (result)
        goto cleanup;

    command_output = generate_output(CommandInfo->pathname, residency);

cleanup:
    Callback(Operation, result, command_output);
    if (command_output)
        globus_free(command_output);
    if (task_id)
        free(task_id);
}

#if (HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9
//
// New stage interface
//

/*
 * Batch stage structure. Keeps state between successive staging calls.
 */
struct batch_stage {
    bfs_bitfile_obj_handle_t bfobjs[BATCH_STAGE_MAX_FILES];
    int count;
};

// SITE STGBEGIN
// 350 SITE STGBEGIN successful. Follow with SITE STGFILE.
// XXX 451 Tape subsystem busy. Please retry SITE STGBEGIN later.
// 503 Command out of sequence. A staging session is already active.
// XXX 550 Failed to begin staging session.

// BatchStage should be pointer-to-NULL on first call
void
stgbegin(
    globus_gfs_operation_t         Operation,    // IN
    globus_gfs_command_info_t   *  CommandInfo,  // IN
    batch_stage_t               ** BatchStage,   // IN/OUT
    commands_callback              Callback)     // IN
{
    if (*BatchStage != NULL)
    {
        Callback(
            Operation,
            GLOBUS_SUCCESS,
            "503 Command out of sequence. A staging session is already active.\r\n");
        return;
    }

    *BatchStage = calloc(1, sizeof(**BatchStage));
    if (*BatchStage == NULL)
    {
        Callback(Operation, GlobusGFSErrorMemory("batch_stage_t"), NULL);
        return;
    }

    Callback(
        Operation,
        GLOBUS_SUCCESS,
        "350 SITE STGBEGIN successful. Follow with SITE STGFILE.\r\n");
}

static globus_result_t
_generate_callback_id(
    globus_gfs_operation_t         Operation,  // IN
    hpss_reqid_t                *  CallbackID) // OUT
{
    char                        *  task_id = NULL;
    unsigned char                  task_id_bytes[UUID_BYTE_COUNT];

    globus_gridftp_server_get_task_id(Operation, &task_id);
    if (!is_valid_uuid(task_id))
    {
        ERROR("No valid task ID available for stage request.");
        return GlobusGFSErrorGeneric("No valid task ID available for stage request.");
    }

    // Convert Task ID to a bytes array
    uuid_str_to_bytes(task_id, task_id_bytes);
    // Convert our bytes array into a callback ID)
    bytes_to_hpss_uuid(task_id_bytes, CallbackID);

    free(task_id);
    return GLOBUS_SUCCESS;
}

static globus_result_t
_submit_batch_stage(
    globus_gfs_operation_t         Operation, // IN
    bfs_bitfile_obj_handle_t    *  BfObjs,    // IN
    int                            Count,     // IN
    hpss_reqid_t                *  RequestID) // OUT
{
    int                            retval = 0;
    hpss_reqid_t                   callback_id;
    hpss_stage_batch_t             stage_batch;
    globus_result_t                result;
    bfs_callback_addr_t            callback_addr;
    hpss_stage_bitfile_list_t      bfids;
    hpss_stage_batch_status_t      status;

    retval = HpssAPI_StageBatchInit(&stage_batch, Count);
    if (retval)
        return hpss_error_to_globus_result(retval);

    for (int i = 0; i < Count; i++)
    {
        retval = HpssAPI_StageBatchInsertBFObj(&stage_batch,
                                               i,
                                               &BfObjs[i],
                                               0, // FromStorageLevel
                                               0, // ToStorageLevel
                                               0, // Offset
                                               0, // Length
                                               BFS_STAGE_ALL);
        if (retval)
        {
            HpssAPI_StageBatchFree(&stage_batch);
            return hpss_error_to_globus_result(retval);
        }
    }

    /*
     * Calculate the callback ID.
     */
    result = _generate_callback_id(Operation, &callback_id);
    if (result != GLOBUS_SUCCESS)
    {
        HpssAPI_StageBatchFree(&stage_batch);
        return result;
    }

    /*
     * Build the callback addr.
     */
    result = _build_callback_addr(callback_id, &callback_addr);
    if (result)
    {
        HpssAPI_StageBatchFree(&stage_batch);
        return result;
    }

    /*
     * Submit the batch stage request.
     */
    // Batch->List.List_len must equal the number of files to stage (no unsed
    // indices in the array) or hpss_StageBatchCallBack() will return -95.
    retval = Hpss_StageBatchCallBack(&stage_batch,   // IN
                                     &callback_addr, // IN
                                     RequestID,      // OUT
                                     &bfids,         // OUT
                                     &status);       // OUT

    if (retval)
    {
        HpssAPI_StageBatchFree(&stage_batch);
        return hpss_error_to_globus_result(retval);
    }

    HpssAPI_StageBatchFree(&stage_batch);
    if (bfids.BFList.BFList_val)
        free(bfids.BFList.BFList_val);
    // Expected: StageStatus=-1406 (HPSS_ENOTALLMOVED)
    HpssAPI_StageStatusFree(&status);
    return GLOBUS_SUCCESS;
}

/*
 * Converts a hpss_reqid_t * (aka a hpss_uuid_t) to a UUID in string format:
 *   ex. hpss_request_id * => "ddfeb23c-53ee-435b-8318-a2c4fb2519d2"
 */
static globus_result_t
hpss_reqid_to_string(
    const hpss_reqid_t          *  RequestID,
    char                        ** UUIDString)
{
    unsigned char bytes[UUID_BYTE_COUNT];
    hpss_uuid_to_bytes(RequestID, bytes);

    *UUIDString = calloc(UUID_STR_COUNT, 1);
    if (*UUIDString == NULL)
        return GlobusGFSErrorMemory("UUIDString");

    uuid_bytes_to_str(bytes, *UUIDString);
    return GLOBUS_SUCCESS;
}


// SITE STGFILE <file>
// 200 File queued. Staging is pending and ready to submit.
// 250 Batch submitted to tape system. Request ID: <request_id>.
// 452 Internal error queuing file. Please retry this file.
// 503 Command out of sequence. Call SITE STGBEGIN first.
// 550 File does not exist or access is denied.
void
stgfile(
    globus_gfs_operation_t         Operation,   // IN
    globus_gfs_command_info_t   *  CommandInfo, // IN
    batch_stage_t               *  BatchStage,  // IN/OUT
    commands_callback              Callback)    // IN
{
    if (BatchStage == NULL)
    {
        Callback(
            Operation,
            GLOBUS_SUCCESS,
            "503 Command out of sequence. Call SITE STGBEGIN first.\r\n");
        return;
    }

    hpss_fileattr_t fattrs;
    int retval = Hpss_FileGetAttributes((char *)CommandInfo->pathname, &fattrs);
    if (retval)
    {
        Callback(
            Operation,
            hpss_error_to_globus_result(retval),
            NULL);
        return;
    }

    if (fattrs.Attrs.Type != NS_OBJECT_TYPE_FILE &&
        fattrs.Attrs.Type != NS_OBJECT_TYPE_HARD_LINK)
    {
        Callback(
            Operation,
            GLOBUS_SUCCESS,
            "550 Path does not refer to a regular file or hard link.\r\n");
        return;
    }

    memcpy(&BatchStage->bfobjs[BatchStage->count++],
           &fattrs.Attrs.BitfileObj,
           sizeof(fattrs.Attrs.BitfileObj));

    if (BatchStage->count < BATCH_STAGE_MAX_FILES)
    {
        Callback(
            Operation,
            GLOBUS_SUCCESS,
            "200 File queued. Stage is pending and ready to submit.\r\n");
        return;
    }

    hpss_reqid_t request_id;
    globus_result_t result = _submit_batch_stage(Operation,
                                                 BatchStage->bfobjs,
                                                 BatchStage->count,
                                                 &request_id);

    if (result)
    {
        // Allow the caller to retry this command
        BatchStage->count--;
        Callback(Operation, result, NULL);
        return;
    }

    // Reset for next use
    BatchStage->count = 0;

    char * request_id_str = hpss_RequestIDtoString(&request_id);
    result = hpss_reqid_to_string(&request_id, &request_id_str);
    if (result)
    {
        Callback(Operation, result, NULL);
        return;
    }

    size_t cnt = snprintf(NULL,
                          0,
                          "250 Batch submitted to tape system. Request ID: %s.\r\n",
                          request_id_str);
    char * response = calloc(cnt+1, 1);
    if (response == NULL)
    {
        Callback(Operation, GlobusGFSErrorMemory("response"), NULL);
        return;
    }
    snprintf(response,
             cnt+1,
             "250 Batch submitted to tape system. Request ID: %s.\r\n",
             request_id_str);

    Callback(Operation, GLOBUS_SUCCESS, response);
    free(request_id_str);
    free(response);
    return;
}

// SITE STGEND
// 200 No pending files to submit to tape system. Stage session ended.
// 250 Remaining files submitted to tape system. Request ID: <request_id>.
// 451 Failed to flush remaining files. Please retry SITE STGEND.
// 503 Command out of sequence. No active staging session to end.
// 550 Failed to end staging session.
void
stgend(
    globus_gfs_operation_t         Operation,    // IN
    globus_gfs_command_info_t   *  CommandInfo,  // IN
    batch_stage_t               ** BatchStage,   // IN/OUT
    commands_callback              Callback)     // IN
{
    if (*BatchStage == NULL)
    {
        Callback(
            Operation,
            GLOBUS_SUCCESS,
            "503 Command out of sequence. No active staging session to end.\r\n");
        return;
    }

    if ((*BatchStage)->count == 0)
    {
        Callback(
            Operation,
            GLOBUS_SUCCESS,
            "200 No pending files to submit to tape system. Stage session ended.\r\n");

        free(*BatchStage);
        *BatchStage = NULL;
        return;
    }

    hpss_reqid_t request_id;
    globus_result_t result = _submit_batch_stage(Operation,
                                                 (*BatchStage)->bfobjs,
                                                 (*BatchStage)->count,
                                                 &request_id);

    if (result)
    {
        // Retryable?
        Callback(Operation, result, NULL);
        return;
    }

    char * request_id_str = hpss_RequestIDtoString(&request_id);
    result = hpss_reqid_to_string(&request_id, &request_id_str);
    if (result)
    {
        Callback(Operation, result, NULL);
        return;
    }
    size_t cnt = snprintf(NULL,
                          0,
                          "250 Remaining files submitted to tape system. Request ID: %s.\r\n",
                          request_id_str);
    char * response = calloc(cnt+1, 1);
    if (response == NULL)
    {
        Callback(Operation, GlobusGFSErrorMemory("response"), NULL);
        return;
    }
    snprintf(response,
             cnt+1,
             "250 Remaining files submitted to tape system. Request ID: %s.\r\n",
             request_id_str);

    Callback(Operation, GLOBUS_SUCCESS, response);
    free(request_id_str);
    free(response);

    free(*BatchStage);
    *BatchStage = NULL;
}

static globus_result_t
_get_request_id_arg(
    globus_gfs_operation_t         Operation,
    globus_gfs_command_info_t   *  CommandInfo,
    hpss_reqid_t                *  RequestID)
{
    globus_result_t result;
    char **         argv   = NULL;
    int             argc   = 0;

    /* Get the command arguments. */
    result = globus_gridftp_server_query_op_info(Operation,
                                                 CommandInfo->op_info,
                                                 GLOBUS_GFS_OP_INFO_CMD_ARGS,
                                                 &argv,
                                                 &argc);

    if (result)
        return GlobusGFSErrorWrapFailed("Unable to get command args", result);

    if (!is_valid_uuid(argv[2]))
    {
        ERROR("Invalid request_id value for STGCHK.");
        return GlobusGFSErrorGeneric("Invalid request_id value for STGCHK");
    }

    // Convert Task ID to a bytes array
    unsigned char request_id_bytes[UUID_BYTE_COUNT];
    uuid_str_to_bytes(argv[2], request_id_bytes);
    // Convert our bytes array into a request ID.
    bytes_to_hpss_uuid(request_id_bytes, RequestID);

    return GLOBUS_SUCCESS;
}

// Original:
// 250: file is resident on disk
// 450: file is not on disk but tape mount request still exists
// 550: an error has occurred, retry SITE STGCHK
// 551: file is not on disk and tape mount does not exist. Reissue ‘SITE STGFILE’ sequence
//      for all non-transferred files on this same request id.

// Updated:
// 211: file is resident on disk
// 213: file is not on disk but tape mount request still exists
// 451: an error has occurred, retry SITE STGCHK
// 550: file is not on disk and tape mount does not exist. Reissue 'SITE STGFILE' sequence
//      for all non-transferred files on this same request id.
void
stgchk(
    globus_gfs_operation_t         Operation,    // IN
    globus_gfs_command_info_t   *  CommandInfo,  // IN
    commands_callback              Callback)     // IN
{
    hpss_reqid_t                   callback_id;
    residency_t                    residency;
    globus_result_t                result;
    hpss_stage_batch_status_t      stage_batch_status;
    hpss_stage_bitfile_list_t      bfids;

    result = check_file_residency(CommandInfo->pathname, &residency);
    if (result)
    {
        Callback(Operation, result, NULL);
        return;
    }

    if (residency == RESIDENCY_RESIDENT)
    {
        Callback(Operation, GLOBUS_SUCCESS, "211 File is resident on disk\r\n");
        return;
    }

    /*
     * Otherwise, check the status of the stage request
     */

    /*
     * The caller should have included the request ID (aka callback ID).
     */
    result = _get_request_id_arg(Operation, CommandInfo, &callback_id);
    if (result != GLOBUS_SUCCESS)
    {
        Callback(Operation, result, NULL);
        return;
    }

    hpss_fileattr_t fattrs;
    int retval = Hpss_FileGetAttributes((char *)CommandInfo->pathname, &fattrs);
    if (retval)
    {
        Callback(Operation, hpss_error_to_globus_result(retval), NULL);
        return;
    }

    bfids.BFList.BFList_len = 1;
    bfids.BFList.BFList_val = &fattrs.Attrs.BitfileObj;

    retval = Hpss_GetBatchAsynchStatus(callback_id,              // IN
                                       &bfids,                   // IN
                                       HPSS_STAGE_STATUS_BFS_SS, // IN
                                       &stage_batch_status);     // OUT
    if (retval)
    {
        Callback(Operation, hpss_error_to_globus_result(retval), NULL);
        return;
    }

    // Stage does not exist: StageStatus=0, RequestId=00000000-0000-0000-0000-000000000000, Position=-1
    // Stage in progress: StageStatus=2, RequestId=8f23a1c6-9ee7-184c-8f78-6edfb1235f3c, Position=-2147483648
    //     RequestID value changes on each request to get status
    if (stage_batch_status.StatusList.StatusList_val[0].StageStatus == 0)
    {
        Callback(Operation,
                 GLOBUS_SUCCESS,
                 "550 File is not on disk and tape mount does not exist. "
                 "Reissue 'SITE STGFILE' sequence for all non-transferred "
                 "files on this same request id.\r\n");
    } else
    {
        Callback(Operation,
                 GLOBUS_SUCCESS,
                 "213 File is not on disk but tape mount request still exists\r\n");
    }

    HpssAPI_StageStatusFree(&stage_batch_status);

}

#endif  //(HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9
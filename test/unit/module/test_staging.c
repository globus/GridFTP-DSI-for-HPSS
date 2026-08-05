/*
 * System includes
 */
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

/*
 * HPSS includes
 */
#include <hpss_api.h>

/*
 * Globus includes
 */
#include <_globus_gridftp_server.h>

/*
 * Project includes
 */
#include <testing.h>
#include <mocking.h>
#include <stage.h>

/*
 * Local includes
 */
#include "driver.h"

struct test_suite_arg {
    int UNUSED;
};
typedef struct test_suite_arg test_suite_arg_t;

/*
 * Function pointers to DSI entry points, loaded once in test_setup().
 */
static void
(*_stgbegin)(
    globus_gfs_operation_t         Operation,
    globus_gfs_command_info_t   *  CommandInfo,
    batch_stage_t               ** BatchStage,
    commands_callback              Callback) = NULL;

static void
(*_stgfile)(
    globus_gfs_operation_t         Operation,
    globus_gfs_command_info_t   *  CommandInfo,
    batch_stage_t               *  BatchStage,
    commands_callback              Callback) = NULL;

static void
(*_stgend)(
    globus_gfs_operation_t         Operation,
    globus_gfs_command_info_t   *  CommandInfo,
    batch_stage_t               ** BatchStage,
    commands_callback              Callback) = NULL;

static void
(*_stgchk)(
    globus_gfs_operation_t         Operation,
    globus_gfs_command_info_t   *  CommandInfo,
    commands_callback              Callback) = NULL;

static void (*_logging_init)() = NULL;

/*
 * HPSS mock functions.
 *
 * These override the HPSS shared library symbols via -rdynamic symbol
 * precedence. Each mock uses GET_RETURN to return a test-configured value
 * and optionally fill out-params via EXPECT_RETURN. When no expectation is
 * queued, GET_RETURN returns the listed default (usually 0 = success).
 */

int
API_StageBatchInit(
    hpss_stage_batch_t          *  Batch,
    int                            Len)
{
    CHECK_PARAMS("Batch", MEMORY, "Len", INT);
    return GET_RETURN(INT, 0, "Batch", MEMORY, Batch, sizeof(*Batch));
}

int
hpss_FileGetAttributes(
    const char                  *  Path,
    hpss_fileattr_t             *  AttrOut)
{
    return GET_RETURN(INT, 0, "AttrOut", MEMORY, AttrOut, sizeof(*AttrOut));
}

int
hpss_FileGetXAttributes(
    const char                  *  Path,
    uint32_t                       Flags,
    uint32_t                       StorageLevel,
    hpss_xfileattr_t            *  AttrOut)
{
    return GET_RETURN(INT, 0, "AttrOut", MEMORY, AttrOut, sizeof(*AttrOut));
}

int
API_StageBatchInsertBFObj(
    hpss_stage_batch_t              *  Batch,
    int                                Idx,
    const bfs_bitfile_obj_handle_t  *  BfObj,
    int                                FromStorageLevel,
    int                                ToStorageLevel,
    u_signed64                         Offset,
    u_signed64                         Length,
    uint32_t                           Flags)
{
    return GET_RETURN(INT, 0);
}

int
hpss_StageBatchCallBack(
    hpss_stage_batch_t          *  Batch,
    bfs_callback_addr_t         *  CallBackPtr,
    hpss_reqid_t                *  ReqID,
    hpss_stage_bitfile_list_t   *  BFIDs,
    hpss_stage_batch_status_t   *  Status)
{
    /* Zero the output structs so _submit_batch_stage's
     *   if (bfids.BFList.BFList_val) free(...)
     *   API_StageStatusFree(&status)
     * both see NULL rather than uninitialised stack garbage. */
    memset(BFIDs,   0, sizeof(*BFIDs));
    memset(Status,  0, sizeof(*Status));
    return GET_RETURN(INT, 0, "ReqID", MEMORY, ReqID, sizeof(*ReqID));
}

void
API_StageBatchFree(
    hpss_stage_batch_t          *  Batch)
{
}

/* _mock_stage_status controls the StageStatus value filled by
 * hpss_GetBatchAsynchStatus. Set it before calling a stgchk test that
 * exercises the non-resident path. Cleared to 0 in test_teardown(). */
static int _mock_stage_status = 0;

int
hpss_GetBatchAsynchStatus(
    hpss_reqid_t                    CallBackId,
    hpss_stage_bitfile_list_t    *  BFIDs,
    hpss_stage_status_type_t        Type,
    hpss_stage_batch_status_t    *  Status)
{
    int retval = GET_RETURN(INT, 0);
    if (retval == 0)
    {
        Status->StatusList.StatusList_val =
            calloc(1, sizeof(*Status->StatusList.StatusList_val));
        Status->StatusList.StatusList_val[0].StageStatus = _mock_stage_status;
        Status->StatusList.StatusList_len = 1;
    }
    return retval;
}

/* API_StageStatusFree frees the StatusList_val allocated by the
 * hpss_GetBatchAsynchStatus mock. free(NULL) is safe for paths that
 * never reach hpss_GetBatchAsynchStatus (e.g. stgfile/stgend submit). */
void
API_StageStatusFree(
    hpss_stage_batch_status_t   *  Status)
{
    free(Status->StatusList.StatusList_val);
    Status->StatusList.StatusList_val = NULL;
}

/*
 * Globus mock functions.
 */

void
globus_gridftp_server_get_task_id(
    globus_gfs_operation_t         op,
    char                        ** task_id)
{
    *task_id = strdup("deadbeef-dead-beef-dead-beefdeadbeef");
}

static char * _stgchk_argv[] =
    {"SITE", "STGCHK", "deadbeef-dead-beef-dead-beefdeadbeef", NULL};

globus_result_t
globus_gridftp_server_query_op_info(
    globus_gfs_operation_t         Operation,
    globus_gfs_op_info_t           OpInfo,
    globus_gfs_op_info_param_t     Param,
    ...)
{
    va_list ap;
    va_start(ap, Param);
    char *** argv = va_arg(ap, char ***);
    int   * argc  = va_arg(ap, int *);
    va_end(ap);

    *argv = _stgchk_argv;
    *argc = 3;
    return GLOBUS_SUCCESS;
}

/*
 * Test fixture helpers.
 */

test_status_t
test_setup(void * Arg)
{
    globus_module_activate(GLOBUS_COMMON_MODULE);

    if (_stgbegin == NULL)
        _stgbegin = lookup_symbol("stgbegin");
    if (_stgfile == NULL)
        _stgfile = lookup_symbol("stgfile");
    if (_stgend == NULL)
        _stgend = lookup_symbol("stgend");
    if (_stgchk == NULL)
        _stgchk = lookup_symbol("stgchk");
    if (_logging_init == NULL)
    {
        _logging_init = lookup_symbol("logging_init");
        _logging_init();
    }

    return TEST_SUCCESS;
}

test_status_t
test_teardown(void * Arg)
{
    reset_expectations();
    _mock_stage_status = 0;
    return TEST_SUCCESS;
}

struct command_callback_args {
    globus_result_t   result;
    char            * command_response;
};
typedef struct command_callback_args command_callback_args_t;

/* command_response may be freed by the caller immediately after this callback
 * returns (e.g. the stgfile/stgend 250 path frees its malloc'd response string).
 * strdup it so the test can safely compare the value after the DSI function
 * returns. The copy leaks, which is acceptable in short-lived test binaries. */
static void
command_callback(
    globus_gfs_operation_t         op,
    globus_result_t                result,
    char                        *  command_response)
{
    command_callback_args_t * args = (command_callback_args_t *) op;
    args->result = result;
    args->command_response = command_response ? strdup(command_response) : NULL;
}

/*
 * Helper: fill xattr for an archived file (tape has more bytes than disk).
 * Used by stgchk tests that need to exercise the non-resident path.
 */
static void
set_archived_xattr(hpss_xfileattr_t * XAttr)
{
    memset(XAttr, 0, sizeof(*XAttr));
    XAttr->Attrs.Type                  = NS_OBJECT_TYPE_FILE;
    XAttr->Attrs.DataLength            = cast64m(1024);
    XAttr->SCAttrib[0].Flags           = BFS_BFATTRS_LEVEL_IS_DISK;
    XAttr->SCAttrib[0].BytesAtLevel    = cast64m(0);
    XAttr->SCAttrib[1].Flags           = BFS_BFATTRS_LEVEL_IS_TAPE;
    XAttr->SCAttrib[1].BytesAtLevel    = cast64m(1024);
}

/*
 * Helper: set up the two EXPECT_RETURNs that all non-resident stgchk tests
 * need: an archived xattr and a successful file-attrs lookup.
 */
static void
setup_stgchk_archived_path(
    hpss_xfileattr_t * FakeXAttr,
    hpss_fileattr_t  * FakeAttrs)
{
    set_archived_xattr(FakeXAttr);
    EXPECT_RETURN("hpss_FileGetXAttributes", WHEN_ONCE, INT, 0,
                  "AttrOut", MEMORY, FakeXAttr);

    memset(FakeAttrs, 0, sizeof(*FakeAttrs));
    EXPECT_RETURN("hpss_FileGetAttributes", WHEN_ONCE, INT, 0,
                  "AttrOut", MEMORY, FakeAttrs);
}

/*
 * SITE STGBEGIN tests
 */

void
test_stgbegin_350_success(void * Arg)
{
    command_callback_args_t args;

    batch_stage_t * batch_stage = NULL;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response,
                      "350 SITE STGBEGIN successful. Follow with SITE STGFILE.\r\n") == 0);

    free(batch_stage);
}

void
test_stgbegin_503_out_of_order(void * Arg)
{
    command_callback_args_t args;

    batch_stage_t * batch_stage = (batch_stage_t *) 0x1;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response,
                      "503 Command out of sequence. A staging session is already active.\r\n") == 0);
}

/*
 * SITE STGFILE tests
 */

void
test_stgfile_503_out_of_order(void * Arg)
{
    command_callback_args_t args;

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    _stgfile((globus_gfs_operation_t)&args, &command_info, NULL, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response,
                      "503 Command out of sequence. Call SITE STGBEGIN first.\r\n") == 0);
}

void
test_stgfile_hpss_error(void * Arg)
{
    command_callback_args_t args;

    batch_stage_t * batch_stage = NULL;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);
    ASSERT(args.result == GLOBUS_SUCCESS);

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    EXPECT_RETURN("hpss_FileGetAttributes", WHEN_ONCE, INT, -ENOENT);

    _stgfile((globus_gfs_operation_t)&args, &command_info, batch_stage, command_callback);

    ASSERT(args.result != GLOBUS_SUCCESS);
    ASSERT(args.command_response == NULL);

    free(batch_stage);
}

void
test_stgfile_550_not_a_file(void * Arg)
{
    command_callback_args_t args;

    batch_stage_t * batch_stage = NULL;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);
    ASSERT(args.result == GLOBUS_SUCCESS);

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    hpss_fileattr_t fake_attrs;
    memset(&fake_attrs, 0, sizeof(fake_attrs));
    fake_attrs.Attrs.Type = NS_OBJECT_TYPE_DIRECTORY;
    EXPECT_RETURN("hpss_FileGetAttributes", WHEN_ONCE, INT, 0,
                  "AttrOut", MEMORY, &fake_attrs);

    _stgfile((globus_gfs_operation_t)&args, &command_info, batch_stage, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response,
                      "550 Path does not refer to a regular file or hard link.\r\n") == 0);

    free(batch_stage);
}

void
test_stgfile_200_file_queued(void * Arg)
{
    command_callback_args_t args;

    batch_stage_t * batch_stage = NULL;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);
    ASSERT(args.result == GLOBUS_SUCCESS);

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    hpss_fileattr_t fake_attrs;
    memset(&fake_attrs, 0, sizeof(fake_attrs));
    fake_attrs.Attrs.Type = NS_OBJECT_TYPE_FILE;
    EXPECT_RETURN("hpss_FileGetAttributes", WHEN_ONCE, INT, 0,
                  "AttrOut", MEMORY, &fake_attrs);

    _stgfile((globus_gfs_operation_t)&args, &command_info, batch_stage, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response,
                      "200 File queued. Stage is pending and ready to submit.\r\n") == 0);

    free(batch_stage);
}

/* The 250 response is triggered when the batch reaches BATCH_STAGE_MAX_FILES
 * (100) entries. We fill the batch with BATCH_STAGE_MAX_FILES-1 queued files
 * via WHEN_ALWAYS, then enqueue the 100th; that last call triggers the submit.
 *
 * A zeroed hpss_reqid_t converts to "00000000-0000-0000-0000-000000000000"
 * via hpss_reqid_to_string (the real function from utils.c). */
void
test_stgfile_250_batch_submitted(void * Arg)
{
    command_callback_args_t args;

    batch_stage_t * batch_stage = NULL;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);
    ASSERT(args.result == GLOBUS_SUCCESS);

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    hpss_fileattr_t fake_attrs;
    memset(&fake_attrs, 0, sizeof(fake_attrs));
    fake_attrs.Attrs.Type = NS_OBJECT_TYPE_FILE;
    EXPECT_RETURN("hpss_FileGetAttributes", WHEN_ALWAYS, INT, 0,
                  "AttrOut", MEMORY, &fake_attrs);

    for (int i = 0; i < BATCH_STAGE_MAX_FILES - 1; i++)
    {
        _stgfile((globus_gfs_operation_t)&args, &command_info, batch_stage, command_callback);
        ASSERT(args.result == GLOBUS_SUCCESS);
    }

    hpss_reqid_t zero_reqid;
    memset(&zero_reqid, 0, sizeof(zero_reqid));
    EXPECT_RETURN("hpss_StageBatchCallBack", WHEN_ONCE, INT, 0,
                  "ReqID", MEMORY, &zero_reqid);

    _stgfile((globus_gfs_operation_t)&args, &command_info, batch_stage, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response,
                      "250 Batch submitted to tape system."
                      " Request ID: 00000000-0000-0000-0000-000000000000.\r\n") == 0);

    free(batch_stage);
}

/*
 * SITE STGEND tests
 */

void
test_stgend_503_out_of_order(void * Arg)
{
    command_callback_args_t args;

    batch_stage_t * batch_stage = NULL;
    _stgend((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response,
                      "503 Command out of sequence. No active staging session to end.\r\n") == 0);
}

void
test_stgend_200_no_pending_files_to_stage(void * Arg)
{
    command_callback_args_t args;

    batch_stage_t * batch_stage = NULL;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    _stgend((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response,
                      "200 No pending files to submit to tape system. Stage session ended.\r\n") == 0);
}

void
test_stgend_hpss_error(void * Arg)
{
    command_callback_args_t args;

    batch_stage_t * batch_stage = NULL;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);
    ASSERT(args.result == GLOBUS_SUCCESS);

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    hpss_fileattr_t fake_attrs;
    memset(&fake_attrs, 0, sizeof(fake_attrs));
    fake_attrs.Attrs.Type = NS_OBJECT_TYPE_FILE;
    EXPECT_RETURN("hpss_FileGetAttributes", WHEN_ONCE, INT, 0,
                  "AttrOut", MEMORY, &fake_attrs);
    _stgfile((globus_gfs_operation_t)&args, &command_info, batch_stage, command_callback);
    ASSERT(args.result == GLOBUS_SUCCESS);

    EXPECT_RETURN("API_StageBatchInit", WHEN_ONCE, INT, -ENOMEM);

    _stgend((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    ASSERT(args.result != GLOBUS_SUCCESS);
    ASSERT(args.command_response == NULL);

    /* stgend does not free the batch on error; clean up here. */
    free(batch_stage);
}

/* Queue one file via stgfile, then call stgend to submit the remaining batch.
 * A zeroed hpss_reqid_t → "00000000-0000-0000-0000-000000000000". */
void
test_stgend_250_remaining_files(void * Arg)
{
    command_callback_args_t args;

    batch_stage_t * batch_stage = NULL;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);
    ASSERT(args.result == GLOBUS_SUCCESS);

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    hpss_fileattr_t fake_attrs;
    memset(&fake_attrs, 0, sizeof(fake_attrs));
    fake_attrs.Attrs.Type = NS_OBJECT_TYPE_FILE;
    EXPECT_RETURN("hpss_FileGetAttributes", WHEN_ONCE, INT, 0,
                  "AttrOut", MEMORY, &fake_attrs);
    _stgfile((globus_gfs_operation_t)&args, &command_info, batch_stage, command_callback);
    ASSERT(args.result == GLOBUS_SUCCESS);

    hpss_reqid_t zero_reqid;
    memset(&zero_reqid, 0, sizeof(zero_reqid));
    EXPECT_RETURN("hpss_StageBatchCallBack", WHEN_ONCE, INT, 0,
                  "ReqID", MEMORY, &zero_reqid);

    _stgend((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response,
                      "250 Remaining files submitted to tape system."
                      " Request ID: 00000000-0000-0000-0000-000000000000.\r\n") == 0);

    ASSERT(batch_stage == NULL);
}

/*
 * SITE STGCHK tests
 */

/* Zero DataLength → check_xattr_residency returns RESIDENT without inspecting
 * the storage class attributes. */
void
test_stgchk_211_file_resident(void * Arg)
{
    command_callback_args_t args;

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    hpss_xfileattr_t fake_xattr;
    memset(&fake_xattr, 0, sizeof(fake_xattr));
    fake_xattr.Attrs.Type = NS_OBJECT_TYPE_FILE;
    /* DataLength == 0 → RESIDENCY_RESIDENT */
    EXPECT_RETURN("hpss_FileGetXAttributes", WHEN_ONCE, INT, 0,
                  "AttrOut", MEMORY, &fake_xattr);

    _stgchk((globus_gfs_operation_t)&args, &command_info, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response, "211 File is resident on disk\r\n") == 0);
}

void
test_stgchk_hpss_error_residency(void * Arg)
{
    command_callback_args_t args;

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    EXPECT_RETURN("hpss_FileGetXAttributes", WHEN_ONCE, INT, -EIO);

    _stgchk((globus_gfs_operation_t)&args, &command_info, command_callback);

    ASSERT(args.result != GLOBUS_SUCCESS);
    ASSERT(args.command_response == NULL);
}

void
test_stgchk_550_no_tape_mount(void * Arg)
{
    command_callback_args_t args;

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    hpss_xfileattr_t fake_xattr;
    hpss_fileattr_t  fake_attrs;
    setup_stgchk_archived_path(&fake_xattr, &fake_attrs);

    _mock_stage_status = 0;
    EXPECT_RETURN("hpss_GetBatchAsynchStatus", WHEN_ONCE, INT, 0);

    _stgchk((globus_gfs_operation_t)&args, &command_info, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response,
                      "550 File is not on disk and tape mount does not exist. "
                      "Reissue 'SITE STGFILE' sequence for all non-transferred "
                      "files on this same request id.\r\n") == 0);
}

void
test_stgchk_213_tape_mount_exists(void * Arg)
{
    command_callback_args_t args;

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    hpss_xfileattr_t fake_xattr;
    hpss_fileattr_t  fake_attrs;
    setup_stgchk_archived_path(&fake_xattr, &fake_attrs);

    _mock_stage_status = 2;
    EXPECT_RETURN("hpss_GetBatchAsynchStatus", WHEN_ONCE, INT, 0);

    _stgchk((globus_gfs_operation_t)&args, &command_info, command_callback);

    ASSERT(args.result == GLOBUS_SUCCESS);
    ASSERT(args.command_response != NULL);
    if (args.command_response != NULL)
        ASSERT(strcmp(args.command_response,
                      "213 File is not on disk but tape mount request still exists\r\n") == 0);
}

void
test_stgchk_hpss_error_batch_status(void * Arg)
{
    command_callback_args_t args;

    globus_gfs_command_info_t command_info;
    memset(&command_info, 0, sizeof(command_info));
    command_info.pathname = "/FOO";

    hpss_xfileattr_t fake_xattr;
    hpss_fileattr_t  fake_attrs;
    setup_stgchk_archived_path(&fake_xattr, &fake_attrs);

    EXPECT_RETURN("hpss_GetBatchAsynchStatus", WHEN_ONCE, INT, -EIO);

    _stgchk((globus_gfs_operation_t)&args, &command_info, command_callback);

    ASSERT(args.result != GLOBUS_SUCCESS);
    ASSERT(args.command_response == NULL);
}

struct test_suite TEST_SUITE = {
    .setup    = test_setup,
    .teardown = test_teardown,
    .test_cases = (struct test_case[]) {
        // SITE STGBEGIN
        {"test_stgbegin_350_success",              test_stgbegin_350_success},
        {"test_stgbegin_503_out_of_order",         test_stgbegin_503_out_of_order},
        // SITE STGFILE
        {"test_stgfile_503_out_of_order",          test_stgfile_503_out_of_order},
        {"test_stgfile_hpss_error",                test_stgfile_hpss_error},
        {"test_stgfile_550_not_a_file",            test_stgfile_550_not_a_file},
        {"test_stgfile_200_file_queued",           test_stgfile_200_file_queued},
        {"test_stgfile_250_batch_submitted",       test_stgfile_250_batch_submitted},
        // SITE STGEND
        {"test_stgend_503_out_of_order",           test_stgend_503_out_of_order},
        {"test_stgend_200_no_pending_files_to_stage", test_stgend_200_no_pending_files_to_stage},
        {"test_stgend_hpss_error",                 test_stgend_hpss_error},
        {"test_stgend_250_remaining_files",        test_stgend_250_remaining_files},
        // SITE STGCHK
        {"test_stgchk_211_file_resident",          test_stgchk_211_file_resident},
        {"test_stgchk_hpss_error_residency",       test_stgchk_hpss_error_residency},
        {"test_stgchk_550_no_tape_mount",          test_stgchk_550_no_tape_mount},
        {"test_stgchk_213_tape_mount_exists",      test_stgchk_213_tape_mount_exists},
        {"test_stgchk_hpss_error_batch_status",    test_stgchk_hpss_error_batch_status},
        {.name = NULL}
    }
};

static test_suite_arg_t test_suite_arg;

void * TEST_SUITE_ARG = &test_suite_arg;

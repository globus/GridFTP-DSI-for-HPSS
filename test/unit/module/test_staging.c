/*
 * System includes
 */
#include <stdlib.h>
#include <stdio.h>

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
    int XXX;
};
typedef struct test_suite_arg test_suite_arg_t;


static void
(*_stgbegin)(
    globus_gfs_operation_t         Operation,        // IN
    globus_gfs_command_info_t   *  CommandInfo,      // IN
    batch_stage_t               ** BatchStage,       // IN/OUT
    commands_callback              Callback) = NULL; // IN

static void
(*_stgend)(
    globus_gfs_operation_t         Operation,        // IN
    globus_gfs_command_info_t   *  CommandInfo,      // IN
    batch_stage_t               ** BatchStage,       // IN/OUT
    commands_callback              Callback) = NULL; // IN

static void
(*_logging_init)() = NULL;

int
API_StageBatchInit(
    hpss_stage_batch_t          *  Batch,
    int                            Len)
{
    CHECK_PARAMS("Batch", MEMORY, "Len", INT);
    return GET_RETURN(INT, 0, "Batch", MEMORY, Batch, sizeof(*Batch));
}

// This is run before each test
test_status_t
test_setup(void * Arg)
{
    // This ensures that globus errors work
    globus_module_activate(GLOBUS_COMMON_MODULE);

    if (_stgbegin == NULL)
        _stgbegin = lookup_symbol("stgbegin");
    if (_stgend == NULL)
        _stgend = lookup_symbol("stgend");
    if (_logging_init == NULL)
    {
        _logging_init = lookup_symbol("logging_init");
        _logging_init();
    }

    return TEST_SUCCESS;
}

struct command_callback_args {
    globus_result_t                result;
    char                        *  command_response;
};
typedef struct command_callback_args command_callback_args_t;

static void
command_callback(
    globus_gfs_operation_t         op,
    globus_result_t                result,
    char                        *  command_response)
{
    //globus_object_t * obj = globus_error_peek(result);
    // 451
    //printf("XXX %d XXX\n", globus_gfs_error_get_ftp_response_code(obj));
    // INTERNAL_ERROR
    //printf("XXX %s XXX\n", globus_gfs_error_get_ftp_response_error_code(obj));

    command_callback_args_t * args = (command_callback_args_t*) op;
    args->result = result;
    args->command_response = command_response;
}

//fprintf(stderr, "%s\n", globus_error_print_chain(globus_error_peek(result)));

void
test_stgbegin_350_success(void * Arg)
{
    //test_suite_arg_t * arg = Arg;
    command_callback_args_t args;

    batch_stage_t * batch_stage = NULL;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    globus_result_t result = args.result;
    ASSERT(result == GLOBUS_SUCCESS);

    const char * response = args.command_response;
    ASSERT(response != NULL);
    if (response != NULL)
        ASSERT(strcmp(response, "350 SITE STGBEGIN successful. Follow with SITE STGFILE.\r\n") == 0);
}

void
test_stgbegin_500_batch_init_failed(void * Arg)
{
    //test_suite_arg_t * arg = Arg;
    command_callback_args_t args;

    EXPECT_RETURN("API_StageBatchInit", WHEN_ONCE, INT, HPSS_EIO);

    batch_stage_t * batch_stage = NULL;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    globus_result_t result = args.result;
    ASSERT(result != GLOBUS_SUCCESS);
    if (result)
    {
        globus_object_t * obj = globus_error_peek(result);

        int code = globus_gfs_error_get_ftp_response_code(obj);
        ASSERT(code == 500);

        char * response = globus_error_print_chain(obj);
        ASSERT(strcmp(response, 
                      "GlobusError: v=1 c=GENERAL_ERROR\n"
                      "HPSS-Reason: HPSS_EIO\n"
                      "HPSS-Function: HpssAPI_StageBatchInit\n") == 0);
    }

    const char * response = args.command_response;
    ASSERT(response == NULL);
}

void
test_stgbegin_503_out_of_order(void * Arg)
{
    //test_suite_arg_t * arg = Arg;
    command_callback_args_t args;

    batch_stage_t * batch_stage = (batch_stage_t *) 0x1;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    globus_result_t result = args.result;
    ASSERT(result == GLOBUS_SUCCESS);

    const char * response = args.command_response;
    ASSERT(response != NULL);
    if (response != NULL)
        ASSERT(strcmp(response, "503 Command out of sequence. A staging session is already active.\r\n") ==  0);
}

void
test_stgend_200_no_pending_files_to_stage(void * Arg)
{
    //test_suite_arg_t * arg = Arg;
    command_callback_args_t args;

    // Successful STGBEGIN
    batch_stage_t * batch_stage = NULL;
    _stgbegin((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    // Successful STGEND w/ no pending stage requests
    _stgend((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    globus_result_t result = args.result;
    ASSERT(result == GLOBUS_SUCCESS);

    const char * response = args.command_response;
    ASSERT(response != NULL);
    if (response != NULL)
        ASSERT(strcmp(response, "200 No pending files to submit to tape system. Stage session ended.\r\n") ==  0);
}

void
test_stgend_503_out_of_order(void * Arg)
{
    //test_suite_arg_t * arg = Arg;
    command_callback_args_t args;

    batch_stage_t * batch_stage = NULL;
    _stgend((globus_gfs_operation_t)&args, NULL, &batch_stage, command_callback);

    globus_result_t result = args.result;
    ASSERT(result == GLOBUS_SUCCESS);

    const char * response = args.command_response;
    ASSERT(response != NULL);
    if (response != NULL)
        ASSERT(strcmp(response, "503 Command out of sequence. No active staging session to end.\r\n") ==  0);
}

struct test_suite TEST_SUITE = {
    .setup = test_setup,
    .teardown = NULL, //test_teardown,
    .test_cases = (struct test_case[]) {
        {"test_stgbegin_350_success", test_stgbegin_350_success},
        {"test_stgbegin_500_batch_init_failed", test_stgbegin_500_batch_init_failed},
        {"test_stgbegin_503_out_of_order", test_stgbegin_503_out_of_order},
        {"test_stgend_200_no_pending_files_to_stage", test_stgend_200_no_pending_files_to_stage},
        {"test_stgend_503_out_of_order", test_stgend_503_out_of_order},
        {.name = NULL}
    }
};

static test_suite_arg_t test_suite_arg;

void * TEST_SUITE_ARG = &test_suite_arg;
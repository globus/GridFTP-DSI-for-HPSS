/*
 * System includes
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

/*
 * Project includes
 */
#include <hpss.h>
#include <stage.h>
#include <logging.h>
#include <authenticate.h>

#if (HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9

static const char * HELP_MSG =
    "Usage: stage [OPTIONS] <PATH>\n"
    "Stage PATH. Wait for stage to complete.\n"
    "\n"
    "OPTIONS:\n"
    "-a <mech>              Authentication mechanism to use to authenticate to HPSS.\n"
    "                       Valid values are krb5 and unix. Required.\n"
    "-p <principal>         The HPSS account to log into HPSS with. Almost always\n"
    "                       is 'hpssftp'. Required.\n"
    "-t <authenticator>     Path to the file containing the principal's keytab.\n"
    "                       Required.\n"
    "-u <username>          The username to switch to once logged into HPSS.\n"
    "                       If used, the -p option must be a HPSS user capable of\n"
    "                       changing process owner.\n"
    "-v <log_level>         The level of additional logging to print to stdout.\n"
    "                       Valid values are: ERROR, WARN, INFO, DEBUG, TRACE, ALL\n"
    "                       or any combination of those values separated by '|'.\n"
    "-i <task_id>           UUID used to compute the callback ID for this request.\n"
    "Examples:\n"
    "$ stage -a unix -p hpssftp -t /var/hpss/etc/keytab -u user1 /my/file\n"
    "\n"
    "$ stage -a krb5 -p user1 -t /home/user1/keytab -v 'ERROR|WARN' /my/file\n"
    "";

void
globus_gridftp_server_get_task_id(
    globus_gfs_operation_t         op,
    char                        ** task_id)
{
    *task_id = strdup("deadbeef-dead-beef-dead-beefdeadbeef");
}

char * StgchkArgv[] = {"SITE", "STGCHK", "deadbeef-dead-beef-dead-beefdeadbeef", NULL};

globus_result_t
globus_gridftp_server_query_op_info(
    globus_gfs_operation_t         Operation,
    globus_gfs_op_info_t           OpInfo,
    globus_gfs_op_info_param_t     Param,
    ...)
{
    va_list args;
    va_start(args, Param);
    char ***argv = va_arg(args, char ***);
    int *argc = va_arg(args, int *);
    va_end(args);

    *argv = StgchkArgv;
    *argc = 3;
    return GLOBUS_SUCCESS;
}

/*
 * Build an FTP response in short format:
 *   200 Ok.
 *
 * Caller will need to free() the return value.
 * ASSERT: code must be a 3 digit base10 value
 * ASSUMPTION: string does not contain '\n'
 */
static char *
_build_ftp_short_response(
    int                            code,
    const char *                   string)
{
    assert(code >= 100 && code < 1000);
    assert(strchr(string, '\n') == NULL);

    // len("XYZ \0") = 5
    char * response = calloc(strlen(string)+5, 1);
    sprintf(response, "%d %s", code, string);
    return response;
}

/*
 * Build an FTP response in long format:
 *   500-GlobusError: v=1 c=GENERAL_ERROR
 *   500-HPSS-Reason: HPSS_ENOATTR
 *   500-HPSS-Function: Hpss_UserAttrGetAttrs
 *   500-HPSS-Last-Error: HPSS_ENOATTR
 *   500-HPSS-Last-Function: API_core_UserAttrGetAttr
 *   500 End.
 *
 * Caller will need to free() the return value.
 * ASSERT: code must be a 3 digit base10 value
 * ASSUMPTION: string does not terminate with '\n\0'
 * ASSUMPTION: string contains at least one '\n'
 */
static char *
_build_ftp_long_response(
    int                            code,
    const char *                   string)
{
    assert(code >= 100 && code < 1000);
    assert(string[strlen(string)-1] != '\n');
    assert(strchr(string, '\n') != NULL);

    // Count the number of lines
    int num_of_lines = 1;
    const char * cptr = string;
    while ((cptr = strchr(cptr, '\n')) != NULL)
    {
        num_of_lines++;
        cptr++;
    }

    int len = strlen(string); // length of input string
    len += 4 * num_of_lines; // len("XYZ-")
    len += 10; // len("XYZ End.\r\n")
    len += 1; // Null terminator

    char * response = calloc(len, 1);

    cptr = string;
    const char * newline;
    while ((newline = strchr(cptr, '\n')) != NULL)
    {
        sprintf(response+strlen(response), "%d-", code);
        strncat(response, cptr, newline-cptr+1);
        cptr = newline + 1;
    }

    sprintf(response+strlen(response), "%d-%s\n%d End.\r\n", code, cptr, code);
    return response;
}

/*
 * Build an FTP response in either short format:
 *   200 Ok.\r\n
 * or long format:
 *   500-GlobusError: v=1 c=GENERAL_ERROR\n
 *   500-HPSS-Reason: HPSS_ENOATTR\n
 *   500-HPSS-Function: Hpss_UserAttrGetAttrs\n
 *   500-HPSS-Last-Error: HPSS_ENOATTR\n
 *   500-HPSS-Last-Function: API_core_UserAttrGetAttr\n
 *   500 End.\r\n
 *
 * Caller will need to free() the return value.
 * ASSERT: code must be a 3 digit base10 value
 * ASSUMPTION: string does not terminate with '\n\0'
 */
static char *
_build_ftp_response(
    int                            code,
    const char *                   string)
{
    assert(code >= 100 && code < 1000);

    if (strchr(string, '\n'))
        return _build_ftp_long_response(code, string);
    return _build_ftp_short_response(code, string);
}

// typedef struct globus_l_gfs_data_operation_s *  globus_gfs_operation_t;
struct globus_l_gfs_data_operation_s {
    int code;
    char * response;
};

static void
_commands_callback(
    globus_gfs_operation_t         op,
    globus_result_t                result,
    char                        *  command_response)
{
    if (result != GLOBUS_SUCCESS)
    {
        globus_object_t * obj = globus_error_peek(result);
        char * error_chain = globus_error_print_chain(obj);
        // globus_error_print_chain() appends a trailing '\n'
        error_chain[strlen(error_chain)-1] = '\0';
        op->code = globus_gfs_error_get_ftp_response_code(obj);
        op->response = _build_ftp_response(op->code, error_chain);
        free(error_chain);
    } else if (command_response)
    {
        op->code = atoi(command_response);
        op->response = strdup(command_response);
    } else
    {
        op->code = 200;
        op->response = strdup("Success");
    }
}

int
main(int argc, char * argv[])
{
    //
    // Parse command line options.
    //
    const char * login_name    = NULL; // -p (aka principal)
    const char * auth_mech     = NULL; // -a (ie unix, krb5)
    const char * authenticator = NULL; // -t (auth_keytab:<path>)
    const char * username      = NULL; // -u (user to setuid to)
    const char * log_level     = NULL; // -v (ALL, INFO, etc)
    const char * path          = NULL;
    const char * task_id       = NULL; // Optional UUID

    int i;
    while ((i = getopt(argc, argv, "p:a:t:u:v:i:")) != -1)
    {
        switch(i)
        {
        case 'a': // unix or krb5
            auth_mech = optarg;
            break;

        case 'p':
            login_name = optarg;
            break;

        case 't':
            authenticator = optarg;
            break;

        case 'u':
            username = optarg;
            break;

        case 'v': // ERROR WARN INFO DEBUG TRACE ALL
            log_level = optarg;
            break;

        case 'i':
            task_id = optarg;
            break;

        case '?':
        default:
            fprintf(stderr, HELP_MSG);
            exit (1);
        }
    }

    if (login_name == NULL)
    {
        fprintf(stderr, "Missing: -p <principal>\n");
        fprintf(stderr, HELP_MSG);
        exit (1);
    }

    if (auth_mech == NULL)
    {
        fprintf(stderr, "Missing: -a [unix|krb5]\n");
        fprintf(stderr, HELP_MSG);
        exit (1);
    }

    if (authenticator == NULL)
    {
        fprintf(stderr, "Missing: -t <authenticator>\n");
        fprintf(stderr, HELP_MSG);
        exit (1);
    }

    if (argc == optind)
    {
        fprintf(stderr, "Missing: path\n");
        fprintf(stderr, HELP_MSG);
        exit (1);
    }

    if ((argc - optind) != 1)
    {
        fprintf(stderr, HELP_MSG);
        exit (1);
    }

    path = argv[optind];

    //
    // Initialize Globus command so that error codes work
    //
    int rc = globus_module_activate(GLOBUS_COMMON_MODULE);
    if (rc != GLOBUS_SUCCESS)
    {
        fprintf(stderr, "Failed to initialize Globus common\n");
        exit(1);
    }

    //
    // Initialize logging based on our command line
    //
    if (log_level)
    {
        const char * env_str_fmt = "GLOBUS_GRIDFTP_SERVER_HPSS_DEBUG=%s,/dev/stdout";
        char * env_string = malloc(strlen(env_str_fmt) + strlen(log_level) + 1);
        sprintf(env_string, env_str_fmt, log_level);
        putenv(env_string);
        // Do not free env_string; it is part of the environment now
        logging_init();
    }

    //
    // Authenticate to HPSS
    //
    globus_result_t result = authenticate((char *)login_name, (char *)auth_mech, (char *)authenticator, (char *)username);
    if (result != GLOBUS_SUCCESS)
    {
        fprintf(stderr, "Failed to log into HPSS.\n");
        fprintf(stderr, "%s\n", globus_error_print_chain(globus_error_peek(result)));
        exit(1);
    }

    struct globus_l_gfs_data_operation_s op;
    globus_gfs_command_info_t command_info;
    command_info.pathname = (char *)path;

    batch_stage_t               *  batch_stage = NULL;
    printf("SITE STGBEGIN\n");
    stgbegin(&op, NULL, &batch_stage, _commands_callback);
    printf("%s\n", op.response);
    free(op.response);
    if (op.code != 350)
        return 1;

    printf("SITE STGFILE %s\n", path);
    stgfile(&op, &command_info, batch_stage, _commands_callback);
    printf("%s\n", op.response);
    free(op.response);
    if (op.code != 200)
        return 1;

    printf("SITE STGEND\n");
    stgend(&op, NULL, &batch_stage, _commands_callback);
    printf("%s\n", op.response);
    free(op.response);
    if (op.code != 250)
        return 1;

    while(1)
    {
        printf("SITE STGCHK %s\n", path);
        stgchk(&op, &command_info, _commands_callback);
        printf("%s\n", op.response);
        free(op.response);
        if (op.code == 211) // Stage completed
            return 0;
        if (op.code != 213) // Stage in progress
            return 1;
        sleep(1);
    }

    return 0;
}

#else // (HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9
int
main()
{
    printf("Not supported on HPSS prior to 9.3\n");
    return 0;
}
#endif // (HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9
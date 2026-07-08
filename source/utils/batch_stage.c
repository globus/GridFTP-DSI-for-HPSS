/*
 * System includes
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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

static void
_commands_callback(
    globus_gfs_operation_t         op,
    globus_result_t                result,
    char                        *  command_response)
{
    //globus_object_t * obj = globus_error_peek(result);
    // 451
    //printf("XXX %d XXX\n", globus_gfs_error_get_ftp_response_code(obj));
    // INTERNAL_ERROR
    //printf("XXX %s XXX\n", globus_gfs_error_get_ftp_response_error_code(obj));
    if (result != GLOBUS_SUCCESS)
    {
        globus_object_t * obj = globus_error_peek(result);
        printf("Reply Code:%d\n", globus_gfs_error_get_ftp_response_code(obj));
        printf("%s\n", globus_gfs_error_get_ftp_response_error_code(obj));

    } else
    {
        printf("%s", command_response);
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

    batch_stage_t               *  batch_stage = NULL;
    printf("SITE STGBEGIN\n");
    stgbegin(NULL, NULL, &batch_stage, _commands_callback);
    printf("SITE STGEND\n");
    stgend(NULL, NULL, &batch_stage, _commands_callback);

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
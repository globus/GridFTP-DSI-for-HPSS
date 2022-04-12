#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <hpss_uuid.h>

#include <stage.h>
#include <logging.h>
#include <authenticate.h>

static const char * HELP_MSG =
    "Usage: stage [OPTIONS] <PATH> <TIMEOUT>\n"
    "Stage PATH. Wait TIMEOUT seconds for stage to complete.\n"
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
    "-i <task_id>           UUID used to compute the request ID for this file.\n"
    "Examples:\n"
    "$ stage -a unix -p hpssftp -t /var/hpss/etc/keytab -u user1 /my/file 5\n"
    "\n"
    "$ stage -a krb5 -p user1 -t /home/user1/keytab -v 'ERROR|WARN' /my/file 1\n"
    "";


static int
_request_id_to_str(const hpss_reqid_t * RequestID, char * Str, size_t Len)
{
#if HPSS_MAJOR_VERSION <= 7
    snprintf(Str, Len, "%u", RequestID);
    return 0;
#else
    return hpss_uuid_snprintf(Str, Len, RequestID);
#endif
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
    int          timeout       = -1;

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

    if ((argc - optind) == 1)
    {
        fprintf(stderr, "Missing: timeout\n");
        fprintf(stderr, HELP_MSG);
        exit (1);
    }

    if ((argc - optind) != 2)
    {
        fprintf(stderr, HELP_MSG);
        exit (1);
    }

    path = argv[optind];
    timeout = atoi(argv[optind+1]);

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

    // XXX Discarding const is a HPSS issue; I should clean this up
    globus_result_t result = authenticate((char *)login_name, (char *)auth_mech, (char *)authenticator, (char *)username);
    if (result != GLOBUS_SUCCESS)
    {
        fprintf(stderr, "Failed to log into HPSS.\n");
        fprintf(stderr, "%s\n", globus_error_print_chain(globus_error_peek(result)));
        exit(1);
    }

    //
    // Stage the file
    //

    hpss_reqid_t request_id;
    residency_t  residency;
    result = stage_ex(path, timeout, task_id, &request_id, &residency);
    if (result)
    {
        fprintf(stderr, "Failed to stage %s\n", path);
        fprintf(stderr, "%s\n", globus_error_print_chain(globus_error_peek(result)));
        exit(1);
    }

    char request_id_str[MAX_UUID_STR_LEN];
    switch(residency)
    {
    case RESIDENCY_ARCHIVED:
        _request_id_to_str(&request_id, request_id_str, sizeof(request_id_str));
        printf("File is being retrived from tape. Request ID: %s\n", request_id_str);
        break;
    case RESIDENCY_RESIDENT:
        printf("File is on disk.\n");
        break;
    case RESIDENCY_TAPE_ONLY:
        printf("File is tape only.\n");
        break;
    }

    return 0;
}

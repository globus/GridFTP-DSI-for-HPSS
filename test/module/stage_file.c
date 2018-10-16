/*
 * System includes.
 */
#include <sys/types.h>
#include <string.h>
#include <pwd.h>

/*
 * HPSS includes
 */
#include <hpss_String.h>
#include <hpss_errno.h>
//include <hpss_mech.h>
#include <hpss_api.h>

#include <stage_test.h>

/* This is used to define the debug print statements. */
GlobusDebugDefine(GLOBUS_GRIDFTP_SERVER_HPSS);

static void
print_result(globus_result_t Result)
{
	globus_object_t * o = globus_error_peek(Result);

	globus_object_printable_string_func_t func;
	func = globus_object_printable_get_string_func(o);

	printf("%s\n", func(o));
}

int
main(int argc, char * argv[])
{
    char * login_name = "hpssftp";
    char * authentication_mech = "krb5";
    char * authenticator = "auth_keytab:/var/hpss/etc/hpss.keytab";

    // Initialize the Globus error module so we can decode GridFTP errors
    globus_module_activate(&globus_i_error_module);
	GlobusDebugInit(GLOBUS_GRIDFTP_SERVER_HPSS, TRACE_STAGING);

    api_config_t api_config;
    int retval = hpss_GetConfiguration(&api_config);
    if (retval != HPSS_E_NOERROR)
    {
        fprintf(stderr, "hpss_GetConfiguration() %s\n", strerror(-retval));
        return 1;
    }

    retval = hpss_AuthnMechTypeFromString(authentication_mech,
	                                     &api_config.AuthnMech);
    if (retval != HPSS_E_NOERROR)
    {
        fprintf(stderr,
		        "hpss_AuthnMechTypeFromString() %s\n", 
		        strerror(-retval));
        return 1;
    }

    hpss_rpc_auth_type_t auth_type;
    retval = hpss_ParseAuthString(authenticator,
                                  &api_config.AuthnMech,
                                  &auth_type,
                                  (void **)&authenticator);
    if (retval != HPSS_E_NOERROR)
    {
        fprintf(stderr, "hpss_ParseAuthString() %s\n", strerror(-retval));
        return 1;
    }

    retval = hpss_SetLoginCred(login_name,
                               api_config.AuthnMech,
                               hpss_rpc_cred_client,
                               auth_type,
                               authenticator);
    if (retval != HPSS_E_NOERROR)
    {
        fprintf(stderr, "hpss_SetLoginCred() %s\n", strerror(-retval));
        return 1;
    }

    retval = hpss_LoadDefaultThreadState(getuid(), hpss_Umask(0), NULL);
    if (retval != HPSS_E_NOERROR)
    {
        fprintf(stderr,
		        "hpss_LoadDefaultThreadState() %s\n",
		        strerror(-retval));
       return 1;
    }

	char * file_list[argc];
	for (int i = 1; i < argc; i++)
	{
		file_list[i-1] = strdup(argv[i]);
	}
	file_list[argc-1] = NULL;

	while (file_list[0] != NULL)
	{
		for (int i = 0; file_list[i] != NULL; )
		{
			residency_t residency;
			globus_result_t result = stage_internal(file_list[i],
			                                        0, 
			                                        &residency);
			if (result != GLOBUS_SUCCESS)
			{
   			 	print_result(result);
				goto cleanup;
			}


			switch (residency)
			{
			case ARCHIVED:
				printf("%s ARCHIVED\n", file_list[i]);
				break;
			case RESIDENT:
				printf("%s RESIDENT\n", file_list[i]);
				break;
			case TAPE_ONLY:
				printf("%s TAPE_ONLY\n", file_list[i]);
				break;
			}

			if (residency == ARCHIVED)
			{
				i++;
				continue;
			}

			memmove(&file_list[i], &file_list[i+1], (argc-i) * sizeof(char *));
		}

		if (file_list[0])
			sleep(30);
	}

cleanup:
    return 0;
}

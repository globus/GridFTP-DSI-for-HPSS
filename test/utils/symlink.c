/*
 * System includes
 */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/*
 * HPSS includes
 */
#include <hpss_String.h>
#include <hpss_api.h>
#include <hpss_errno.h>
#include <hpss_mech.h>

/*
 * Local includes.
 */
#include "error.h"

/*
 * You need a keytab file in order to authenticate to HPSS. On systems
 * with unix authentication, use hpss_unix_keytab:
 * % hpss_unix_keytab -f <keytab file> -P [add | update] <principal>
 *
 * On kerberos systems, you should be able to point this at a kerberos
 * keytab file. You can construct a kerberos keytab like this:
 * > ktutil
 * ktutil:  addent -password -p <username>@<domain> -k 1 -e aes256-cts
 * Password for <username>@<domain>: [enter your password]
 * ktutil:  wkt <keytab_file>
 * ktutil:  quit
 */

_error
authenticate(const char * LoginName, const char * KeytabFile)
{
    // authn_mech_string = unix|krb5|gsi|spkm
    char * authn_mech_string = hpss_Getenv("HPSS_API_AUTHN_MECH");
    if (!authn_mech_string)
        authn_mech_string = hpss_Getenv("HPSS_PRIMARY_AUTHN_MECH");

    // authn_mech = hpss_authn_mech_krb5 | hpss_authn_mech_unix
    hpss_authn_mech_t authn_mech; 
    int retval = hpss_AuthnMechTypeFromString(authn_mech_string, &authn_mech);
    if (retval != HPSS_E_NOERROR)
        return _ERROR("hpss_AuthnMechTypeFromString()", -retval);

    /* Now log into HPSS using our configured 'super user' */
    retval = hpss_SetLoginCred((char *)LoginName,
                               authn_mech,
                               hpss_rpc_cred_client,
                               hpss_rpc_auth_type_keytab,
                               (char *)KeytabFile);

    if (retval != HPSS_E_NOERROR)
    {
        fprintf(stderr, "hpss_SetLoginCred()\n");
        return _ERROR("hpss_SetLoginCred()", -retval);
    }

    return _ERROR(NULL, 0);
}


int
main(int argc, char * argv[])
{
    if (argc != 5)
    {
        fprintf(stderr,
                "%s <user> <keytab_file> <target> <linkname>\n", 
                argv[0]);
        return 1;
    }

    _error e = authenticate(argv[1], argv[2]);
    if (e.Value != 0)
    {
        fprintf(stderr, "failed\n");
        return 1;
    }

    hpss_stat_t buf;       
    int retval = hpss_Stat("/home/jasonalt/", &buf);
    if (retval != HPSS_E_NOERROR)
    {
        fprintf(stderr, "hpss_Stat() failed\n");
        return 1;
    }

    retval = hpss_Symlink(argv[3], argv[4]);
    if (retval != HPSS_E_NOERROR)
    {
        fprintf(stderr, "hpss_Symlink(): %s\n", strerror(-retval));
        return 1;
    }

    return 0;
}

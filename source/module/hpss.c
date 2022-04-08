/*
 * System includes.
 */
#include <stdbool.h>
#include <string.h>

/*
 * Local includes.
 */
#include "hpss_error.h"
#include "hpss_log.h"
#include "hpss.h"

/*******************************************************************************
 *   HPSS DEVELOPMENT NOTES
 *
 *  - Don't trigger calls to API_ClientAPIInit() before Hpss_SetLoginCred(),
 *    it'll just hang. This includes any calls that communicate with the 
 *    core server and other less obvious calls like Hpss_ClearLastHPSSErrno().
 *
 ******************************************************************************/

/* Toggle additional functionality once we are logged in. */
static bool _SetLoginCredCompleted = false;

/*
 * HPSS calls that are only used internally by our HPSS API interface.
 */
static void
Hpss_ClearLastHPSSErrno(void);

static hpss_errno_state_t
Hpss_GetLastHPSSErrno();
 
#define HPSS_ERROR(r, l)  \
    r >= 0 ? r : hpss_error_put((hpss_error_t){r, __func__, l})

void
HpssAPI_ConvertTimeToPosixTime(
    const hpss_Attrs_t          *  Attrs,
    timestamp_sec_t             *  Atime,
    timestamp_sec_t             *  Mtime,
    timestamp_sec_t             *  Ctime)
{
    API_ENTER("API_ConvertTimeToPosixTime",
              "Attrs=%s Atime=%s Mtime=%s Ctime=%s",
              HPSS_ATTRS_T_PTR(Attrs),
              PTR(Atime),
              PTR(Mtime),
              PTR(Ctime));

#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    API_ConvertTimeToPosixTime((hpss_Attrs_t *)Attrs, Atime, Mtime, Ctime);
#else
    API_ConvertTimeToPosixTime(Attrs, Atime, Mtime, Ctime);
#endif

    API_EXIT("API_ConvertTimeToPosixTime",
             "Atime=%s Mtime=%s Ctime=%s",
              TIMESTAMP_SEC_T_PTR(Atime),
              TIMESTAMP_SEC_T_PTR(Mtime),
              TIMESTAMP_SEC_T_PTR(Ctime));
}
 
signed32
Hpss_AuthnMechTypeFromString(
   const char              *  AuthnMechString,
   hpss_authn_mech_t       *  AuthnMech)
{
    API_ENTER("hpss_AuthnMechTypeFromString",
              "AuthnMechString=5s AuthnMech=%s",
              CHAR_PTR(AuthnMechString),
              PTR(AuthnMech));

    Hpss_ClearLastHPSSErrno();
    signed32 rv = hpss_AuthnMechTypeFromString(AuthnMechString, AuthnMech);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_AuthnMechTypeFromString",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_AUTHN_MECH_T_PTR(AuthnMech));
    return rv;
}

/* Caller must free returned string */
char *
Hpss_BuildLevelString(void)
{
    API_ENTER("hpss_BuildLevelString", "");
    char * rv = hpss_BuildLevelString();
    API_EXIT("hpss_BuildLevelString",
             "return_value=%s",
             CHAR_PTR(rv));
    return rv;
}

int
Hpss_Chmod(
    const char                  *  Path,
    mode_t                         Mode)
{
    API_ENTER("hpss_Chmod", "Path=%s Mode=%s", CHAR_PTR(Path), MODE_T(Mode));

    Hpss_ClearLastHPSSErrno();
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_Chmod((char *)Path, Mode);
#else
    int rv = hpss_Chmod(Path, Mode);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Chmod",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

char *
Hpss_ChompXMLHeader(
    char                        * XML,
    char                        * Header)
{
    API_ENTER("hpss_ChompXMLHeader",
              "XML=%s Header=%s",
              CHAR_PTR(XML),
              CHAR_PTR(Header));
    char * rv = hpss_ChompXMLHeader(XML, Header);
    API_EXIT("hpss_ChompXMLHeader", "return_value=%s", CHAR_PTR(rv));
    return rv;
}

static void
Hpss_ClearLastHPSSErrno(void)
{
    if (_SetLoginCredCompleted)
        hpss_ClearLastHPSSErrno();
}

static hpss_errno_state_t
Hpss_GetLastHPSSErrno()
{
    if (_SetLoginCredCompleted)
        return hpss_GetLastHPSSErrno();

    static hpss_errno_state_t es;
    memset(&es, 0, sizeof(es));
    return es;
}

int
Hpss_Close(int Fildes)
{
    API_ENTER("hpss_Close", "Fildes=%s", INT(Fildes));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_Close(Fildes);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Close",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_Closedir(int Dirdes)
{
    API_ENTER("hpss_Closedir", "Dirdes=%s", INT(Dirdes));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_Closedir(Dirdes);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Closedir",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_FileGetAttributes(
    const char                  *  Path,
    hpss_fileattr_t             *  AttrOut)
{
    API_ENTER("hpss_FileGetAttributes",
              "Path=%s AttrOut=%s",
              CHAR_PTR(Path),
              PTR(AttrOut));

    memset(AttrOut, 0, sizeof(*AttrOut));

    Hpss_ClearLastHPSSErrno();
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_FileGetAttributes((char *)Path, AttrOut);
#else
    int rv = hpss_FileGetAttributes(Path, AttrOut);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_FileGetAttributes",
             "return_value=%s last_hpss_errno=%s AttrOut=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_FILEATTR_T(AttrOut));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_FileGetXAttributes(
    const char                  *  Path,
    uint32_t                       Flags,
    uint32_t                       StorageLevel,
    hpss_xfileattr_t            *  AttrOut)
{
    API_ENTER("hpss_FileGetXAttributes",
              "Path=%s Flags=%s StorageLevel=%s AttrOut=%s",
              CHAR_PTR(Path),
              HEX(Flags),
              HEX(StorageLevel),
              PTR(AttrOut));

    memset(AttrOut, 0, sizeof(*AttrOut));

    Hpss_ClearLastHPSSErrno();
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_FileGetXAttributes((char *)Path, Flags, StorageLevel, AttrOut);
#else
    int rv = hpss_FileGetXAttributes(Path, Flags, StorageLevel, AttrOut);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_FileGetXAttributes",
             "return_value=%s last_hpss_errno=%s AttrOut=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_XFILEATTR_T_PTR(AttrOut));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_FilesetGetAttributes(
    const char                  *  Name,            // IN
    const uint64_t              *  FilesetId,       // IN
    const ns_ObjHandle_t        *  FilesetHandle,   // IN
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION == 4
    const hpss_uuid_t           *  CoreServerUUID,  // IN
#else
    const hpss_srvr_id_t        *  CoreServerID,    // IN
#endif
    ns_FilesetAttrBits_t           FilesetAttrBits, // IN
    ns_FilesetAttrs_t           *  FilesetAttrs)    // OUT
{
    API_ENTER("hpss_FilesetGetAttributes",
              "Name=%s "            // const char *
              "FilesetId=%s "       // uint64_t *
              "FilesetHandle=%s "   // const ns_ObjHandle_t *
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION == 4
              "CoreServerUUID=%s "  // const hpss_uuid_t *
#else
              "CoreServerID=%s "    // const hpss_srvr_id_t *
#endif
              "FilesetAttrBits=%s " // ns_FilesetAttrBits_t
              "FilesetAttrs=%s",    // ns_FilesetAttrs_t *
              CHAR_PTR(Name),
              UNSIGNED64_PTR(FilesetId),
              NS_OBJHANDLE_T_PTR(FilesetHandle),
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION == 4
              HPSS_UUID_T_PTR(CoreServerUUID),
#else
              HPSS_SRVR_ID_T_PTR(CoreServerID),
#endif
              NS_FILESETATTRBITS_T(FilesetAttrBits),
              PTR(FilesetAttrs));

    memset(FilesetAttrs, 0, sizeof(*FilesetAttrs));

    Hpss_ClearLastHPSSErrno();
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_FilesetGetAttributes((char *)Name,
                                       (uint64_t *)FilesetId,
                                       (ns_ObjHandle_t *)FilesetHandle,
                                       (hpss_uuid_t *)CoreServerUUID,
#else
    int rv = hpss_FilesetGetAttributes(Name,
                                       FilesetId,
                                       FilesetHandle,
                                       CoreServerID,
#endif
                                       FilesetAttrBits,
                                       FilesetAttrs);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_FilesetGetAttributes",
             "return_value=%s last_hpss_errno=%s FilesetAttrs=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             NS_FILESETATTRS_T(FilesetAttrs));
    return HPSS_ERROR(rv, errno_state);
}

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
int
Hpss_GetAsynchStatus(
    signed32                       CallBackId,
    hpssoid_t                   *  BitfileID,
    signed32                    *  Status)
{
    API_ENTER("hpss_GetAsynchStatus",
              "CallBackId=%s "
              "BitfileID=%s "
              "Status=%s",
              UNSIGNED(CallBackId),
              HPSSOID_T_PTR(BitfileID),
              PTR(Status));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_GetAsynchStatus(CallBackId, BitfileID, Status);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_GetAsynchStatus",
             "return_value=%s last_hpss_errno=%s Status=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             SIGNED_PTR(Status));
    return HPSS_ERROR(rv, errno_state);
}
#else
int
Hpss_GetAsyncStatus(
    hpss_reqid_t                   CallBackId,
    bfs_bitfile_obj_handle_t    *  BitfileObj,
    int32_t                     *  Status)
{
    API_ENTER("hpss_GetAsyncStatus",
              "CallBackId=%s "
              "BitfileObj=%s",
              HPSS_REQID_T(CallBackId),
              BFS_BITFILE_OBJ_HANDLE_T_PTR(BitfileObj));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_GetAsyncStatus(CallBackId, BitfileObj, Status);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_GetAsyncStatus",
             "return_value=%s last_hpss_errno=%s Status=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             SIGNED_PTR(Status));
    return HPSS_ERROR(rv, errno_state);
}
#endif

char *
Hpss_Getenv(const char *Env)
{
    API_ENTER("hpss_Getenv", "Env=%s", CHAR_PTR(Env));

    char * rv = hpss_Getenv(Env);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Getenv",
             "return_value=%s last_hpss_errno=%s",
             CHAR_PTR(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return rv;
}

int
Hpss_GetConfiguration(
    api_config_t                *  ConfigOut)
{
    API_ENTER("hpss_GetConfiguration", "ConfigOut=%s", PTR(ConfigOut));

    memset(ConfigOut, 0, sizeof(*ConfigOut));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_GetConfiguration(ConfigOut);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_GetConfiguration",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             API_CONFIG_T_PTR(ConfigOut));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_GetThreadUcred(
    sec_cred_t                  *  RetUcred)
{
    API_ENTER("hpss_GetThreadUcred", "RetUcred=%s", PTR(RetUcred));

    memset(RetUcred, 0, sizeof(*RetUcred));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_GetThreadUcred(RetUcred);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_GetThreadUcred",
             "return_value=%s last_hpss_errno=%s RetUcred=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             SEC_CRED_T_PTR(RetUcred));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_LoadDefaultThreadState(
    uid_t                     UserID,
    mode_t                    Umask,
    char                   *  ClientFullName)
{
    API_ENTER("hpss_LoadDefaultThreadState",
              "UserID=%s Umask=%s ClientFullName=%s",
              UID_T(UserID),
              MODE_T(Umask),
              CHAR_PTR(ClientFullName));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_LoadDefaultThreadState(UserID, Umask, ClientFullName);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_LoadDefaultThreadState",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_Lstat(
    const char                  *  Path,
    hpss_stat_t                 *  Buf)
{
    API_ENTER("hpss_Lstat", "Path=%s Buf=%s", CHAR_PTR(Path), PTR(Buf));

    memset(Buf, 0, sizeof(*Buf));

    Hpss_ClearLastHPSSErrno();
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    int rv = hpss_Lstat((char *)Path, Buf);
#else
    int rv = hpss_Lstat(Path, Buf);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Lstat",
             "return_value=%s last_hpss_errno=%s Buf=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_STAT_T_PTR(Buf));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_Mkdir(
    const char                  *  Path,
    mode_t                         Mode)
{
    API_ENTER("hpss_Mkdir", "Path=%s Mode=%s", CHAR_PTR(Path), MODE_T(Mode));

    Hpss_ClearLastHPSSErrno();
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    int rv = hpss_Mkdir((char *)Path, Mode);
#else
    int rv = hpss_Mkdir(Path, Mode);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Mkdir",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_net_getaddrinfo(
    const char                  *  hostname,
    const char                  *  service,
    int                            flags,
    hpss_ipproto_t                 protocol,
    hpss_sockaddr_t             *  addr,
    char                        *  errbuf,
    size_t                         errbuflen)
{
    API_ENTER("hpss_net_getaddrinfo",
              "hostname=%s "
              "service=%s "
              "flags=%s "
              "protocol=%s "
              "addr=%s "
              "errbuf=%s "
              "errbuflen=%s",
              CHAR_PTR(hostname),
              CHAR_PTR(service),
              HEX(flags),
              UNSIGNED(protocol),
              PTR(addr),
              PTR(errbuf),
              UNSIGNED(errbuflen));

    memset(addr, 0, sizeof(*addr));
    memset(errbuf, 0, errbuflen);

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_net_getaddrinfo(hostname,
                                  service,
                                  flags,
                                  protocol,
                                  addr,
                                  errbuf,
                                  errbuflen);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_net_getaddrinfo",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_Open(
    const char                  * Path,
    int                           Oflag,
    mode_t                        Mode,
    const hpss_cos_hints_t      * HintsIn,
    const hpss_cos_priorities_t * HintsPri,
    hpss_cos_hints_t            * HintsOut)
{
    API_ENTER("hpss_Open",
              "Path=%s "
              "Oflag=%s "
              "Mode=%s "
              "HintsIn=%s "
              "HintsPri=%s "
              "HintsOut=%s",
              CHAR_PTR(Path),
              HEX(Oflag),
              MODE_T(Mode),
              HPSS_COS_HINTS_T_PTR(HintsIn),
              HPSS_COS_PRIORITIES_T_PTR(HintsPri),
              PTR(HintsOut));

    memset(HintsOut, 0, sizeof(*HintsOut));

    Hpss_ClearLastHPSSErrno();
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    int rv = hpss_Open((char *)Path,
                       Oflag,
                       Mode,
                       (hpss_cos_hints_t *)HintsIn,
                       (hpss_cos_priorities_t *)HintsPri,
                       HintsOut);
#else
    int rv = hpss_Open(Path, Oflag, Mode, HintsIn, HintsPri, HintsOut);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Open",
             "return_value=%s last_hpss_errno=%s HintsOut=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_COS_HINTS_T_PTR(HintsOut));
    return HPSS_ERROR(rv, errno_state);
}

#if HPSS_MAJOR_VERSION >= 8
int // int
Hpss_OpendirHandle(
    const ns_ObjHandle_t        *  DirHandle,
    const sec_cred_t            *  Ucred)
{
    API_ENTER("hpss_OpendirHandle",
              "DirHandle=%s Ucred=%s",
              NS_OBJHANDLE_T_PTR(DirHandle),
              SEC_CRED_T_PTR(Ucred));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_OpendirHandle(DirHandle, Ucred);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_OpendirHandle",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}
#endif

signed32
Hpss_ParseAuthString(
    char                   *  AuthenticatorString, // IN
    hpss_authn_mech_t      *  AuthnMechanism,      // OUT
    hpss_rpc_auth_type_t   *  AuthenticatorType,   // OUT
    void                   ** Authenticator)       // OUT
{
    API_ENTER("hpss_ParseAuthString",
              "AuthenticatorString=%s "
              "AuthnMechanism=%s "
              "AuthenticatorType=%s "
              "Authenticator=%s",
              CHAR_PTR(AuthenticatorString),
              PTR(AuthnMechanism),
              PTR(AuthenticatorType),
              PTR(Authenticator));

    Hpss_ClearLastHPSSErrno();
    signed32 rv = hpss_ParseAuthString(AuthenticatorString,
                                       AuthnMechanism,
                                       AuthenticatorType,
                                       Authenticator);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_ParseAuthString",
             "return_value=%s "
             "last_hpss_errno=%s "
             "AuthnMechanism=%s "
             "AuthenticatorType=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_AUTHN_MECH_T_PTR(AuthnMechanism),
             HPSS_RPC_AUTH_TYPE_T_PTR(AuthenticatorType));
    return rv;
}

int
Hpss_PIOEnd(
    hpss_pio_grp_t                 StripeGroup)
{
    API_ENTER("hpss_PIOEnd", "StripeGroup=%s", PTR(StripeGroup));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_PIOEnd(StripeGroup);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_PIOEnd",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_PIOExecute(
    int                            Fd,
    uint64_t                       FileOffset,
    uint64_t                       Size,
    const hpss_pio_grp_t           StripeGroup,
    hpss_pio_gapinfo_t          *  GapInfo,
    uint64_t                    *  BytesMoved)
{
    API_ENTER("hpss_PIOExecute",
              "Fd=%s "
              "FileOffset=%s "
              "Size=%s "
              "StripeGroup=<opague> "
              "GapInfo=%s "
              "BytesMoved=%s",
              INT(Fd),
              UNSIGNED64(FileOffset),
              UNSIGNED64(Size),
              PTR(GapInfo),
              PTR(BytesMoved));

    memset(GapInfo, 0, sizeof(*GapInfo));
    // Don't clear BytesMoved, it is used as a regression test in pio.c

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_PIOExecute(Fd,
                             FileOffset,
                             Size,
                             StripeGroup,
                             GapInfo,
                             BytesMoved);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_PIOExecute",
             "return_value=%s last_hpss_errno=%s GapInfo=%s BytesMoved=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_PIO_GAPINFO_T_PTR(GapInfo),
             UNSIGNED64_PTR(BytesMoved));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_PIOExportGrp(
    const hpss_pio_grp_t           StripeGroup,
    void                        ** Buffer,
    unsigned int                *  BufLength)
{
    API_ENTER("hpss_PIOExportGrp",
              "StripeGroup=%s Buffer=%s BufLength=%s",
              PTR(StripeGroup),
              PTR(Buffer),
              PTR(BufLength));

    memset(Buffer, 0, *BufLength);

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_PIOExportGrp(StripeGroup, Buffer, BufLength);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_PIOExportGrp",
             "return_value=%s last_hpss_errno=%s BufLength=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             UNSIGNED_PTR(BufLength));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_PIOImportGrp(
    const void                  *  Buffer,
    unsigned int                   BufLength,
    hpss_pio_grp_t              *  StripeGroup)
{
    API_ENTER("hpss_PIOImportGrp",
              "Buffer=%s BufLength=%s StripeGroup=%s",
              PTR(Buffer),
              UNSIGNED(BufLength),
              PTR(StripeGroup));

    // hpss_pio_grp_t is opaque, no way to zero it

    Hpss_ClearLastHPSSErrno();
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_PIOImportGrp((void *)Buffer, BufLength, StripeGroup);
#else
    int rv = hpss_PIOImportGrp(Buffer, BufLength, StripeGroup);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_PIOImportGrp",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_PIORegister(
    uint32_t                       StripeElement,   // IN
    const hpss_sockaddr_t       *  DataNetSockAddr, // IN
    void                        *  DataBuffer,      // IN
    uint32_t                       DataBufLen,      // IN
    hpss_pio_grp_t                 StripeGroup,     // IN
    const hpss_pio_cb_t            IOCallback,      // IN
    const void                  *  IOCallbackArg)   // IN
{
    API_ENTER("hpss_PIORegister",
              "StripeElement=%s "
              "DataNetSockAddr=%s "
              "DataBuffer=%s "
              "DataBufLen=%s "
              "StripeGroup=%s "
              "IOCallback=%s "
              "IOCallbackArg=%s",
              UNSIGNED(StripeElement),
              PTR(DataNetSockAddr),
              PTR(DataBuffer),
              UNSIGNED(DataBufLen),
              PTR(StripeGroup),
              PTR(IOCallback),
              PTR(IOCallbackArg));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_PIORegister(StripeElement,
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
                              (hpss_sockaddr_t *) DataNetSockAddr,
#else
                              DataNetSockAddr,
#endif
                              DataBuffer,
                              DataBufLen,
                              StripeGroup,
                              IOCallback,
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
                              (void *) IOCallbackArg);
#else
                              IOCallbackArg);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_PIORegister",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_PIOStart(
    hpss_pio_params_t           *  InputParams,
    hpss_pio_grp_t              *  StripeGroup)
{
    API_ENTER("hpss_PIOStart",
              "InputParams=%s StripeGroup=%s",
              HPSS_PIO_PRARAMS_T_PTR(InputParams),
              PTR(StripeGroup));

    // hpss_pio_grp_t is opaque, no way to zero it

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_PIOStart(InputParams, StripeGroup);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_PIOStart",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

#if HPSS_MAJOR_VERSION >= 8
int
Hpss_ReadAttrsPlus(
    int                            Dirdes,     // IN
    uint64_t                       OffsetIn,   // IN
    uint32_t                       BufferSize, // IN
    hpss_readdir_flags_t           Flags,      // IN
    uint32_t                    *  End,        // OUT
    uint64_t                    *  OffsetOut,  // OUT
    ns_DirEntry_t               *  DirentPtr)  // OUT
{
    API_ENTER("hpss_ReadAttrsPlus",
              "Dirdes=%s "
              "OffsetIn=%s "
              "BufferSize=%s "
              "Flags=%s "
              "End=%s "
              "OffsetOut=%s "
              "DirentPtr=%s",
              INT(Dirdes),
              UNSIGNED64(OffsetIn),
              UNSIGNED(BufferSize),
              HPSS_READDIR_FLAGS_T(Flags),
              PTR(End),
              PTR(OffsetOut),
              PTR(DirentPtr));

    memset(DirentPtr, 0, BufferSize);

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_ReadAttrsPlus(Dirdes,
                                OffsetIn,
                                BufferSize,
                                Flags,
                                End,
                                OffsetOut,
                                DirentPtr);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_ReadAttrsPlus",
             "return_value=%s "
             "last_hpss_errno=%s "
             "End=%s "
             "OffsetOut=%s "
             "DirentPtr=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             UNSIGNED_PTR(End),
             UNSIGNED64_PTR(OffsetOut),
             rv > 0 ? NS_DIRENTRY_T_ARRAY(DirentPtr, rv) : "{}");
    return HPSS_ERROR(rv, errno_state);
}
#else
int
Hpss_ReadAttrsHandle(
    const ns_ObjHandle_t        *  ObjHandle,     // IN
    uint64_t                       OffsetIn,      // IN
    const sec_cred_t            *  Ucred,         // IN
    uint32_t                       BufferSize,    // IN
    uint32_t                       GetAttributes, // IN
    uint32_t                    *  End,           // OUT
    uint64_t                    *  OffsetOut,     // OUT
    ns_DirEntry_t               *  DirentPtr)     // OUT
{
    API_ENTER("hpss_ReadAttrsHandle",
              "ObjHandle=%s "
              "OffsetInfo=%s "
              "Ucred=%s "
              "BufferSize=%s "
              "GetAttributes=%s "
              "End=%s "
              "OffsetOut=%s "
              "DirentPtr=%s",
              NS_OBJHANDLE_T_PTR(ObjHandle),
              UNSIGNED64(OffsetIn),
              SEC_CRED_T_PTR(Ucred),
              UNSIGNED(BufferSize),
              UNSIGNED(GetAttributes),
              PTR(End),
              PTR(OffsetOut),
              PTR(DirentPtr));

    memset(DirentPtr, 0, BufferSize);

    Hpss_ClearLastHPSSErrno();
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_ReadAttrsHandle((ns_ObjHandle_t *) ObjHandle,
#else
    int rv = hpss_ReadAttrsHandle(ObjHandle,
#endif
                                  OffsetIn,
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
                                  (sec_cred_t *)Ucred,
#else
                                  Ucred,
#endif
                                  BufferSize,
                                  GetAttributes,
                                  End,
                                  OffsetOut,
                                  DirentPtr);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_ReadAttrsHandle",
             "return_value=%s "
             "last_hpss_errno=%s "
             "End=%s "
             "OffsetOut=%s "
             "DirentPtr=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             UNSIGNED_PTR(End),
             UNSIGNED64_PTR(OffsetOut),
             rv > 0 ? NS_DIRENTRY_T_ARRAY(DirentPtr, rv) : "[]");
    return HPSS_ERROR(rv, errno_state);
}
#endif

int
Hpss_Readlink(
    const char                  *  Path,
    char                        *  Contents,
    size_t                         BufferSize)
{
    API_ENTER("hpss_Readlink",
              "Path=%s BufferSize",
              CHAR_PTR(Path),
              UNSIGNED(BufferSize));

    memset(Contents, 0, BufferSize);

    Hpss_ClearLastHPSSErrno();

#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_Readlink((char *)Path, Contents, BufferSize);
#else
    int rv = hpss_Readlink(Path, Contents, BufferSize);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Readlink",
             "return_value=%s "
             "last_hpss_errno=%s "
             "Contents=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             CHAR_PTR(Contents));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_ReadlinkHandle(
    const ns_ObjHandle_t        * ObjHandle,
    const char                  * Path,
    char                        * Contents,
    size_t                        BufferSize,
    const sec_cred_t            * Ucred)
{
    API_ENTER("hpss_ReadlinkHandle",
              "ObjHandle=%s Path=%s BufferSize=%s Ucred=%s",
              NS_OBJHANDLE_T_PTR(ObjHandle),
              CHAR_PTR(Path),
              UNSIGNED(BufferSize),
              SEC_CRED_T_PTR(Ucred));

    memset(Contents, 0, BufferSize);

    Hpss_ClearLastHPSSErrno();

#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_ReadlinkHandle((ns_ObjHandle_t *)ObjHandle,
                                 (char *)Path,
                                 Contents,
                                 BufferSize,
                                 (sec_cred_t *)Ucred);
#else
    int rv = hpss_ReadlinkHandle(ObjHandle,
                                 Path,
                                 Contents,
                                 BufferSize,
                                 Ucred);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_ReadlinkHandle",
             "return_value=%s "
             "last_hpss_errno=%s "
             "Contents=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             CHAR_PTR(Contents));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_Rename(
    const char                  *  Old,
    const char                  *  New)
{
    API_ENTER("hpss_Rename", "Old=%s New=%s", CHAR_PTR(Old), CHAR_PTR(New));

    Hpss_ClearLastHPSSErrno();
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_Rename((char *)Old, (char *)New);
#else
    int rv = hpss_Rename(Old, New);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Rename",
             "return_value=%s "
             "last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_Rmdir(
    const char                  *  Path)
{
    API_ENTER("hpss_Rmdir", "Path=%s", CHAR_PTR(Path));

    Hpss_ClearLastHPSSErrno();
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_Rmdir((char *)Path);
#else
    int rv = hpss_Rmdir(Path);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Rmdir",
             "return_value=%s "
             "last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_SetConfiguration(
    const api_config_t          * ConfigIn)
{
    API_ENTER("hpss_SetConfiguration",
              "ConfigIn=%s",
              API_CONFIG_T_PTR(ConfigIn));

    Hpss_ClearLastHPSSErrno();
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_SetConfiguration((api_config_t *)ConfigIn);
#else
    int rv = hpss_SetConfiguration(ConfigIn);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_SetConfiguration",
             "return_value=%s "
             "last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_SetCOSByHints(
    int                            Fildes,   // IN
    uint32_t                       Flags,    // IN
    const hpss_cos_hints_t      *  HintsPtr, // IN
    const hpss_cos_priorities_t *  PrioPtr,  // IN
    hpss_cos_md_t               *  COSPtr)   // OUT
{
    API_ENTER("hpss_SetCOSByHints",
              "Fildes=%s Flags=%s HintsPtr=%s PrioPtr=%s COSPtr=%s",
              INT(Fildes),
              HEX(Flags),
              HPSS_COS_HINTS_T_PTR(HintsPtr),
              HPSS_COS_PRIORITIES_T_PTR(PrioPtr),
              PTR(COSPtr));

    memset(COSPtr, 0, sizeof(*COSPtr));

    Hpss_ClearLastHPSSErrno();
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_SetCOSByHints(Fildes,
                                Flags,
                                (hpss_cos_hints_t *)HintsPtr,
                                (hpss_cos_priorities_t *)PrioPtr,
                                COSPtr);
#else
    int rv = hpss_SetCOSByHints(Fildes, Flags, HintsPtr, PrioPtr, COSPtr);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_SetCOSByHints",
             "return_value=%s "
             "last_hpss_errno=%s "
             "COSPtr=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_COS_MD_T_PTR(COSPtr));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_SetLoginCred(
    char                   *  PrincipalName, // IN
    hpss_authn_mech_t         Mechanism,     // IN
    hpss_rpc_cred_type_t      CredType,      // IN
    hpss_rpc_auth_type_t      AuthType,      // IN
    void                   *  Authenticator) // IN
{
    API_ENTER("hpss_SetLoginCred",
              "PrincipalName=%s "
              "Mechanism=%s "
              "CredType=%s "
              "AuthType=%s "
              "Authenticator=%s",
              CHAR_PTR(PrincipalName),
              HPSS_AUTHN_MECH_T(Mechanism),
              HPSS_RPC_CRED_TYPE_T(CredType),
              HPSS_RPC_AUTH_TYPE_T(AuthType),
              PTR(Authenticator));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_SetLoginCred(PrincipalName,
                                         Mechanism,
                                         CredType,
                                         AuthType,
                                         Authenticator);

    /* Enable post-login functionality. */
    if (rv == HPSS_E_NOERROR)
        _SetLoginCredCompleted = true;

    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_SetLoginCred",
             "return_value=%s "
             "last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_StageCallBack(
    const char                  *  Path,         // IN
    uint64_t                       Offset,       // IN
    uint64_t                       Length,       // IN
    uint32_t                       StorageLevel, // IN
    bfs_callback_addr_t         *  CallBackPtr,  // IN
    uint32_t                       Flags,        // IN
    hpss_reqid_t                *  ReqID,        // IN/OUT
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    hpssoid_t                   *  BitfileID)    // OUT
#else
    bfs_bitfile_obj_handle_t    *  BitfileObj)   // OUT
#endif
{
    API_ENTER("hpss_StageCallBack",
              "Path=%s "
              "Offset=%s "
              "Length=%s "
              "StorageLevel = %s "
              "CallBackPtr=%s "
              "Flags=%s "
              "ReqId=%s "
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
              "BitfileID=%s",
#else
              "BitfileObj=%s",
#endif
              CHAR_PTR(Path),
              UNSIGNED64(Offset),
              UNSIGNED64(Length),
              UNSIGNED(StorageLevel),
              PTR(CallBackPtr),
              HEX(Flags),
              HPSS_REQID_T_PTR(ReqID),
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
              HPSSOID_T_PTR(BitfileID));
#else
              PTR(BitfileObj));
#endif

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    memset(BitfileID, 0, sizeof(*BitfileID));
#else
    memset(BitfileObj, 0, sizeof(*BitfileObj));
#endif

    Hpss_ClearLastHPSSErrno();
#if HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4
    int rv = hpss_StageCallBack((char *)Path,
#else
    int rv = hpss_StageCallBack(Path,
#endif
                                Offset,
                                Length,
                                StorageLevel,
                                CallBackPtr,
                                Flags,
                                ReqID,
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
                                BitfileID);
#else
                                BitfileObj);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_StageCallBack",
             "return_value=%s "
             "last_hpss_errno=%s "
             "ReqID=%s "
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
             "BitfileID",
#else
             "BitfileObj=%s",
#endif
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_REQID_T_PTR(ReqID),
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
             HPSSOID_T_PTR(BitfileID));
#else
             BFS_BITFILE_OBJ_HANDLE_T_PTR(BitfileObj));
#endif
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_Stat(
    const char                  * Path,
    hpss_stat_t                 * Buf)
{
    API_ENTER("hpss_Stat", "Path=%s", CHAR_PTR(Path));

    memset(Buf, 0, sizeof(*Buf));

    Hpss_ClearLastHPSSErrno();
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    int rv = hpss_Stat((char *)Path, Buf);
#else
    int rv = hpss_Stat(Path, Buf);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Stat",
             "return_value=%s last_hpss_errno=%s Buf=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_STAT_T_PTR(Buf));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_Symlink(
    const char                  *  Contents,
    const char                  *  Path)
{
    API_ENTER("hpss_Symlink",
              "Path=%s Contents=%s",
              CHAR_PTR(Path),
              CHAR_PTR(Contents));

    Hpss_ClearLastHPSSErrno();
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    int rv = hpss_Symlink((char *)Contents, (char *)Path);
#else
    int rv = hpss_Symlink(Contents, Path);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Symlink",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_Truncate(
    const char                  *  Path,
    uint64_t                       Length)
{
    API_ENTER("hpss_Truncate",
              "Path=%s Length=%s",
              CHAR_PTR(Path),
              UNSIGNED64(Length));

    Hpss_ClearLastHPSSErrno();
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    int rv = hpss_Truncate((char *)Path, Length);
#else
    int rv = hpss_Truncate(Path, Length);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Truncate",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

mode_t
Hpss_Umask(mode_t CMask)
{
    API_ENTER("hpss_Umask", "CMask=%s", MODE_T(CMask));

    Hpss_ClearLastHPSSErrno();
    mode_t rv = hpss_Umask(CMask);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Umask",
             "return_value=%s last_hpss_errno=%s",
             MODE_T(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return rv;
}

int
Hpss_Unlink(
    const char                  *  Path)
{
    API_ENTER("hpss_Unlink", "Path=%s", CHAR_PTR(Path));

    Hpss_ClearLastHPSSErrno();
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    int rv = hpss_Unlink((char *)Path);
#else
    int rv = hpss_Unlink(Path);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Unlink",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

int
Hpss_UnlinkHandle(
    const ns_ObjHandle_t        *  ObjHandle,
    const char                  *  Path,
    const sec_cred_t            *  Ucred)
{
    API_ENTER("hpss_UnlinkHandle",
              "ObjHandle=%s Path=%s Ucred=%s",
              NS_OBJHANDLE_T_PTR(ObjHandle),
              CHAR_PTR(Path),
              SEC_CRED_T_PTR(Ucred));

    Hpss_ClearLastHPSSErrno();
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    int rv = hpss_UnlinkHandle((ns_ObjHandle_t *)ObjHandle,
                               (char *)Path,
                               (sec_cred_t *)Ucred);
#else
    int rv = hpss_UnlinkHandle(ObjHandle, Path, Ucred);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_UnlinkHandle",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
int
Hpss_UserAttrGetAttrs(
    char                        * Path,    // IN
    hpss_userattr_list_t        * Attr,    // IN/OUT
    int                           XMLFlag) // IN
{
    API_ENTER("hpss_UserAttrGetAttrs",
              "Path=%s Attr=%s XMLFlag=%s",
              CHAR_PTR(Path),
              HPSS_USERATTR_LIST_T_PTR(Attr),
              INT(XMLFlag));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_UserAttrGetAttrs((char *)Path, Attr, XMLFlag);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_UserAttrGetAttrs",
             "return_value=%s last_hpss_errno=%s Attr=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_USERATTR_LIST_T_PTR(Attr));
    return HPSS_ERROR(rv, errno_state);
}
#else
int
Hpss_UserAttrGetAttrs(
    const char                  * Path,    // IN
    hpss_userattr_list_t        * Attr,    // IN/OUT
    int                           XMLFlag, // IN
    int                           XMLSize) // IN
{
    API_ENTER("hpss_UserAttrGetAttrs",
              "Path=%s Attr=%s XMLFlag=%s XMLSize=%s",
              CHAR_PTR(Path),
              HPSS_USERATTR_LIST_T_PTR(Attr),
              INT(XMLFlag),
              INT(XMLSize));

    Hpss_ClearLastHPSSErrno();
    int rv = hpss_UserAttrGetAttrs(Path, Attr, XMLFlag, XMLSize);
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_UserAttrGetAttrs",
             "return_value=%s last_hpss_errno=%s Attr=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state),
             HPSS_USERATTR_LIST_T_PTR(Attr));
    return HPSS_ERROR(rv, errno_state);
}
#endif

int
Hpss_UserAttrSetAttrs(
    const char                  * Path,   // IN
    const hpss_userattr_list_t  * Attr,   // IN
    const char                  * Schema) // IN
{
    API_ENTER("hpss_UserAttrSetAttrs",
              "Path=%s Attr=%s Schema=%s",
              CHAR_PTR(Path),
              HPSS_USERATTR_LIST_T_PTR(Attr),
              CHAR_PTR(Schema));

    Hpss_ClearLastHPSSErrno();
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    int rv = hpss_UserAttrSetAttrs((char *)Path,
                                   (hpss_userattr_list_t *)Attr,
                                   (char *)Schema);
#else
    int rv = hpss_UserAttrSetAttrs(Path, Attr, Schema);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_UserAttrSetAttrs",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}


int
Hpss_Utime(
    const char                  *  Path,
    const struct utimbuf        *  Times)
{
    API_ENTER("hpss_Utime",
              "Path=%s Times=%s",
              CHAR_PTR(Path),
              STRUCT_UTIMBUF_PTR(Times));

    Hpss_ClearLastHPSSErrno();
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    int rv = hpss_Utime((char *)Path, Times);
#else
    int rv = hpss_Utime(Path, Times);
#endif
    hpss_errno_state_t errno_state = Hpss_GetLastHPSSErrno();

    API_EXIT("hpss_Utime",
             "return_value=%s last_hpss_errno=%s",
             INT(rv),
             HPSS_ERRNO_STATE_T(errno_state));
    return HPSS_ERROR(rv, errno_state);
}

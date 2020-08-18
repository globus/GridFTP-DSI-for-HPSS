/*
 * System includes.
 */
#include <stdbool.h>
#include <string.h>

/*
 * Local includes.
 */
#include "logging.h"
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

void
Hpss_ClearLastHPSSErrno(void);

static int
_FindBestErrno(int Errno)
{
    if (Errno >= 0)
        return Errno;

    hpss_errno_state_t last_errno_state = hpss_GetLastHPSSErrno();
    if (last_errno_state.hpss_errno != 0)
        return last_errno_state.hpss_errno;
    return Errno;
}

void
HpssAPI_ConvertTimeToPosixTime(
    const hpss_Attrs_t          *  Attrs,
    timestamp_sec_t             *  Atime,
    timestamp_sec_t             *  Mtime,
    timestamp_sec_t             *  Ctime)
{
    API_ConvertTimeToPosixTime(Attrs, Atime, Mtime, Ctime);
}
 
signed32
Hpss_AuthnMechTypeFromString(
   const char              *  AuthnMechString,
   hpss_authn_mech_t       *  AuthnMech)
{
    Hpss_ClearLastHPSSErrno();
    signed32 return_value = hpss_AuthnMechTypeFromString(AuthnMechString,
                                                         AuthnMech);
    return _FindBestErrno(return_value);
}

int
Hpss_Chmod(
    const char                  *  Path,
    mode_t                         Mode)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Chmod(Path, Mode);
    return _FindBestErrno(return_value);
}

char*
Hpss_ChompXMLHeader(
    char                        * XML,
    char                        * Header)
{
    char * return_value = hpss_ChompXMLHeader(XML, Header);
    return return_value;
}

void
Hpss_ClearLastHPSSErrno(void)
{
    if (_SetLoginCredCompleted)
        hpss_ClearLastHPSSErrno();
}

int
Hpss_Close(int Fildes)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Close(Fildes);
    return _FindBestErrno(return_value);
}

int
Hpss_Closedir(int Dirdes)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Closedir(Dirdes);
    return _FindBestErrno(return_value);
}

int
Hpss_FileGetAttributes(
    const char                  *  Path,
    hpss_fileattr_t             *  AttrOut)
{
    memset(AttrOut, 0, sizeof(*AttrOut));

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_FileGetAttributes(Path, AttrOut);
    return _FindBestErrno(return_value);
}

int
Hpss_FileGetXAttributes(
    const char                  *  Path,
    uint32_t                       Flags,
    uint32_t                       StorageLevel,
    hpss_xfileattr_t            *  AttrOut)
{
    memset(AttrOut, 0, sizeof(*AttrOut));

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_FileGetXAttributes(Path,
                                               Flags,
                                               StorageLevel,
                                               AttrOut);
    return _FindBestErrno(return_value);
}

int
Hpss_FilesetGetAttributes(
    const char                  *  Name,
    const uint64_t              *  FilesetId,
    const ns_ObjHandle_t        *  FilesetHandle,
    const hpss_srvr_id_t        *  CoreServerID,
    ns_FilesetAttrBits_t           FilesetAttrBits,
    ns_FilesetAttrs_t           *  FilesetAttrs)
{
    memset(FilesetAttrs, 0, sizeof(*FilesetAttrs));

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_FilesetGetAttributes(Name,
                                                 FilesetId,
                                                 FilesetHandle,
                                                 CoreServerID,
                                                 FilesetAttrBits,
                                                 FilesetAttrs);
    return _FindBestErrno(return_value);
}

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION < 4)
int
Hpss_GetAsynchStatus(
    signed32                       CallBackId,
    hpssoid_t                   *  BitfileID,
    signed32                    *  Status)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_GetAsynchStatus(CallBackId, BitfileID, Status);
    return _FindBestErrno(return_value);
}
#else
int
Hpss_GetAsyncStatus(
    hpss_reqid_t                   CallBackId,
    bfs_bitfile_obj_handle_t    *  BitfileObj,
    int32_t                     *  Status)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_GetAsyncStatus(CallBackId, BitfileObj, Status);
    return _FindBestErrno(return_value);
}
#endif

char *
Hpss_Getenv(const char *Env)
{
    char * return_value = hpss_Getenv(Env);
    return return_value;
}

int
Hpss_GetConfiguration(
    api_config_t                *  ConfigOut)
{
    memset(ConfigOut, 0, sizeof(*ConfigOut));

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_GetConfiguration(ConfigOut);
    return _FindBestErrno(return_value);
}

int
Hpss_GetThreadUcred(
    sec_cred_t                  *  RetUcred)
{
    memset(RetUcred, 0, sizeof(*RetUcred));

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_GetThreadUcred(RetUcred);
    return _FindBestErrno(return_value);
}

int
Hpss_LoadDefaultThreadState(
    uid_t                     UserID,
    mode_t                    Umask,
    char                   *  ClientFullName)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_LoadDefaultThreadState(UserID,
                                                   Umask,
                                                   ClientFullName);
    return _FindBestErrno(return_value);
}

int
Hpss_Lstat(
    const char                  *  Path,
    hpss_stat_t                 *  Buf)
{
    memset(Buf, 0, sizeof(*Buf));

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Lstat(Path, Buf);
    return _FindBestErrno(return_value);
}

int
Hpss_Mkdir(
    const char                  *  Path,
    mode_t                         Mode)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Mkdir(Path, Mode);
    return _FindBestErrno(return_value);
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
    memset(addr, 0, sizeof(*addr));
    memset(errbuf, 0, errbuflen);

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_net_getaddrinfo(hostname,
                                            service,
                                            flags,
                                            protocol,
                                            addr,
                                            errbuf,
                                            errbuflen);
    return _FindBestErrno(return_value);
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
    memset(HintsOut, 0, sizeof(*HintsOut));

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Open(Path,
                                 Oflag,
                                 Mode,
                                 HintsIn,
                                 HintsPri,
                                 HintsOut);
    return _FindBestErrno(return_value);
}

#if HPSS_MAJOR_VERSION >= 8
int
Hpss_OpendirHandle(
    const ns_ObjHandle_t        *  DirHandle,
    const sec_cred_t            *  Ucred)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_OpendirHandle(DirHandle, Ucred);
    return _FindBestErrno(return_value);
}
#endif

signed32
Hpss_ParseAuthString(
    char                   *  AuthenticatorString,
    hpss_authn_mech_t      *  AuthnMechanism,
    hpss_rpc_auth_type_t   *  AuthenticatorType,
    void                   ** Authenticator)
{
    Hpss_ClearLastHPSSErrno();
    signed32 return_value = hpss_ParseAuthString(AuthenticatorString,
                                                 AuthnMechanism,
                                                 AuthenticatorType,
                                                 Authenticator);
    return _FindBestErrno(return_value);
}

int
Hpss_PIOEnd(
    hpss_pio_grp_t                 StripeGroup)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_PIOEnd(StripeGroup);
    return _FindBestErrno(return_value);
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
    memset(GapInfo, 0, sizeof(*GapInfo));
    // Don't clear BytesMoved, it is used as a regression test in pio.c

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_PIOExecute(Fd,
                                       FileOffset,
                                       Size,
                                       StripeGroup,
                                       GapInfo,
                                       BytesMoved);
    return _FindBestErrno(return_value);
}

int
Hpss_PIOExportGrp(
    const hpss_pio_grp_t           StripeGroup,
    void                        ** Buffer,
    unsigned int                *  BufLength)
{
    memset(Buffer, 0, *BufLength);

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_PIOExportGrp(StripeGroup, Buffer, BufLength);
    return _FindBestErrno(return_value);
}

int
Hpss_PIOImportGrp(
    const void                  *  Buffer,
    unsigned int                   BufLength,
    hpss_pio_grp_t              *  StripeGroup)
{
    Hpss_ClearLastHPSSErrno();
    // hpss_pio_grp_t is opaque, no way to zero it
    int return_value = hpss_PIOImportGrp(Buffer, BufLength, StripeGroup);
    return _FindBestErrno(return_value);
}

int
Hpss_PIORegister(
    uint32_t                       StripeElement,
    const hpss_sockaddr_t       *  DataNetSockAddr,
    void                        *  DataBuffer,
    uint32_t                       DataBufLen,
    hpss_pio_grp_t                 StripeGroup,
    const hpss_pio_cb_t            IOCallback,
    const void                  *  IOCallbackArg)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_PIORegister(StripeElement,
                                        DataNetSockAddr,
                                        DataBuffer,
                                        DataBufLen,
                                        StripeGroup,
                                        IOCallback,
                                        IOCallbackArg);
    return _FindBestErrno(return_value);
}

int
Hpss_PIOStart(
    hpss_pio_params_t           *  InputParams,
    hpss_pio_grp_t              *  StripeGroup)
{
    Hpss_ClearLastHPSSErrno();
    // hpss_pio_grp_t is opaque, no way to zero it
    int return_value = hpss_PIOStart(InputParams, StripeGroup);
    return _FindBestErrno(return_value);
}

#if HPSS_MAJOR_VERSION >= 8
int
Hpss_ReadAttrsPlus(
    int                            Dirdes,
    uint64_t                       OffsetIn,
    uint32_t                       BufferSize,
    hpss_readdir_flags_t           Flags,
    uint32_t                    *  End,
    uint64_t                    *  OffsetOut,
    ns_DirEntry_t               *  DirentPtr)
{
    memset(DirentPtr, 0, BufferSize);

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_ReadAttrsPlus(Dirdes,
                                          OffsetIn,
                                          BufferSize,
                                          Flags,
                                          End,
                                          OffsetOut,
                                          DirentPtr);
    return _FindBestErrno(return_value);
}
#else
int
Hpss_ReadAttrsHandle(
    const ns_ObjHandle_t        *  ObjHandle,
    uint64_t                       OffsetIn,
    const sec_cred_t            *  Ucred,
    uint32_t                       BufferSize,
    uint32_t                       GetAttributes,
    uint32_t                    *  End,
    uint64_t                    *  OffsetOut,
    ns_DirEntry_t               *  DirentPtr)
{
    memset(DirentPtr, 0, BufferSize);

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_ReadAttrsHandle(ObjHandle,
                                            OffsetIn,
                                            Ucred,
                                            BufferSize,
                                            GetAttributes,
                                            End,
                                            OffsetOut,
                                            DirentPtr);
    return _FindBestErrno(return_value);
}
#endif

int
Hpss_Readlink(
    const char                  *  Path,
    char                        *  Contents,
    size_t                         BufferSize)
{
    memset(Contents, 0, BufferSize);

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Readlink(Path, Contents, BufferSize);
    return _FindBestErrno(return_value);
}

int
Hpss_ReadlinkHandle(
    const ns_ObjHandle_t        *  ObjHandle,
    const char                  *  Path,
    char                        *  Contents,
    size_t                         BufferSize,
    const sec_cred_t            *  Ucred)
{
    memset(Contents, 0, BufferSize);

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_ReadlinkHandle(ObjHandle,
                                           Path,
                                           Contents,
                                           BufferSize,
                                           Ucred);
    return _FindBestErrno(return_value);
}

int
Hpss_Rename(
    const char                  *  Old,
    const char                  *  New)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Rename(Old, New);
    return _FindBestErrno(return_value);
}

int
Hpss_Rmdir(
    const char                  *  Path)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Rmdir(Path);
    return _FindBestErrno(return_value);
}

int
Hpss_SetConfiguration(
    const api_config_t          * ConfigIn)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_SetConfiguration(ConfigIn);
    return _FindBestErrno(return_value);
}

int
Hpss_SetCOSByHints(
    int                            Fildes,
    uint32_t                       Flags,
    const hpss_cos_hints_t      *  HintsPtr,
    const hpss_cos_priorities_t *  PrioPtr,
    hpss_cos_md_t               *  COSPtr)
{
    memset(COSPtr, 0, sizeof(*COSPtr));

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_SetCOSByHints(Fildes,
                                          Flags,
                                          HintsPtr,
                                          PrioPtr,
                                          COSPtr);
    return _FindBestErrno(return_value);
}

int
Hpss_SetLoginCred(
    char                   *  PrincipalName,
    hpss_authn_mech_t         Mechanism,
    hpss_rpc_cred_type_t      CredType,
    hpss_rpc_auth_type_t      AuthType,
    void                   *  Authenticator)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_SetLoginCred(PrincipalName,
                                         Mechanism,
                                         CredType,
                                         AuthType,
                                         Authenticator);

    /* Enable post-login functionality. */
    if (return_value == HPSS_E_NOERROR)
        _SetLoginCredCompleted = true;
    return _FindBestErrno(return_value);
}

int
Hpss_StageCallBack(
    const char                  *  Path,
    uint64_t                       Offset,
    uint64_t                       Length,
    uint32_t                       StorageLevel,
    bfs_callback_addr_t         *  CallBackPtr,
    uint32_t                       Flags,
    hpss_reqid_t                *  ReqID,
    bfs_bitfile_obj_handle_t    *  BitfileObj)
{
    memset(BitfileObj, 0, sizeof(*BitfileObj));

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_StageCallBack(Path,
                                          Offset,
                                          Length,
                                          StorageLevel,
                                          CallBackPtr,
                                          Flags,
                                          ReqID,
                                          BitfileObj);
    return _FindBestErrno(return_value);
}

int
Hpss_Stat(
    const char                  * Path,
    hpss_stat_t                 * Buf)
{
    memset(Buf, 0, sizeof(*Buf));

    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Stat(Path, Buf);
    return _FindBestErrno(return_value);
}

int
Hpss_Symlink(
    const char                  *  Contents,
    const char                  *  Path)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Symlink(Contents, Path);
    return _FindBestErrno(return_value);
}

int
Hpss_Truncate(
    const char                  *  Path,
    uint64_t                       Length)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Truncate(Path, Length);
    return _FindBestErrno(return_value);
}

mode_t
Hpss_Umask(mode_t CMask)
{
    Hpss_ClearLastHPSSErrno();
    mode_t return_value = hpss_Umask(CMask);
    return _FindBestErrno(return_value);
}

int
Hpss_Unlink(
    const char                  *  Path)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Unlink(Path);
    return _FindBestErrno(return_value);
}

int
Hpss_UnlinkHandle(
    const ns_ObjHandle_t        *  ObjHandle,
    const char                  *  Path,
    const sec_cred_t            *  Ucred)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_UnlinkHandle(ObjHandle, Path, Ucred);
    return _FindBestErrno(return_value);
}

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION < 4)
int
Hpss_UserAttrGetAttrs(
    char                        * Path,
    hpss_userattr_list_t        * Attr,
    int                           XMLFlag)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_UserAttrGetAttrs(Path, Attr, XMLFlag);
    return _FindBestErrno(return_value);
}
#else
int
Hpss_UserAttrGetAttrs(
    const char                  * Path,
    hpss_userattr_list_t        * Attr,
    int                           XMLFlag,
    int                           XMLSize)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_UserAttrGetAttrs(Path, Attr, XMLFlag, XMLSize);
    return _FindBestErrno(return_value);
}
#endif

int
Hpss_UserAttrSetAttrs(
    const char                  * Path,
    const hpss_userattr_list_t  * Attr,
    const char                  * Schema)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_UserAttrSetAttrs(Path, Attr, Schema);
    return _FindBestErrno(return_value);
}


int
Hpss_Utime(
    const char                  *  Path,
    const struct utimbuf        *  Times)
{
    Hpss_ClearLastHPSSErrno();
    int return_value = hpss_Utime(Path, Times);
    return _FindBestErrno(return_value);
}

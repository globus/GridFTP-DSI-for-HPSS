#ifndef _HPSS_H_
#define _HPSS_H_

/*
 * HPSS includes
 */
#include <hpss_api.h>
#include <hpss_xml.h>
#include <hpss_errno.h>
#include <hpss_Getenv.h>
#include <hpss_mech.h>
#include <hpss_String.h>
#include <hpss_version.h>

void
HpssAPI_ConvertTimeToPosixTime(
    const hpss_Attrs_t          *  Attrs,
    timestamp_sec_t             *  Atime,
    timestamp_sec_t             *  Mtime,
    timestamp_sec_t             *  Ctime);

signed32
Hpss_AuthnMechTypeFromString(
   const char                   *  AuthnMechString,
   hpss_authn_mech_t            *  AuthnMech);

int
Hpss_Chmod(
    const char                  *  Path,
    mode_t                         Mode);

char *
Hpss_ChompXMLHeader(
    char                        *  XML,
    char                        *  Header);

int
Hpss_Close(int Fildes);

int
Hpss_Closedir(int Dirdes);

int
Hpss_FileGetAttributes(
    const char                  *  Path,
    hpss_fileattr_t             *  AttrOut);

int
Hpss_FileGetXAttributes(
    const char                  *  Path,
    uint32_t                       Flags,
    uint32_t                       StorageLevel,
    hpss_xfileattr_t            *  AttrOut);

int
Hpss_FilesetGetAttributes(
    const char                  *  Name,
    const uint64_t              *  FilesetId,
    const ns_ObjHandle_t        *  FilesetHandle,
#if (HPSS_MAJOR_VERSION == 7)
    const hpss_uuid_t           *  CoreServerUUID
#else
    const hpss_srvr_id_t        *  CoreServerID,
#endif
    ns_FilesetAttrBits_t           FilesetAttrBits,
    ns_FilesetAttrs_t           *  FilesetAttrs);

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION < 4)
int
Hpss_GetAsynchStatus(
    signed32                       CallBackId,
    hpssoid_t                   *  BitfileID,
    signed32                    *  Status);
#else
int
Hpss_GetAsyncStatus(
    hpss_reqid_t                   CallBackId,
    bfs_bitfile_obj_handle_t    *  BitfileObj,
    int32_t                     *  Status);
#endif

char *
Hpss_Getenv(const char *Env);

int
Hpss_GetConfiguration(
    api_config_t                *  ConfigOut);

int
Hpss_GetThreadUcred(
    sec_cred_t                  *  RetUcred);

int
Hpss_LoadDefaultThreadState(
    uid_t                          UserID,
    mode_t                         Umask,
    char                        *  ClientFullName);

int
Hpss_Lstat(
    const char                  *  Path,
    hpss_stat_t                 *  Buf);

int
Hpss_Mkdir(
    const char                  *  Path,
    mode_t                         Mode);

int
Hpss_net_getaddrinfo(
    const char                  *  hostname,
    const char                  *  service,
    int                            flags,
    hpss_ipproto_t                 protocol,
    hpss_sockaddr_t             *  addr,
    char                        *  errbuf,
    size_t                         errbuflen);

int
Hpss_Open(
    const char                  *  Path,
    int                            Oflag,
    mode_t                         Mode,
    const hpss_cos_hints_t      *  HintsIn,
    const hpss_cos_priorities_t *  HintsPri,
    hpss_cos_hints_t            *  HintsOut);

#if HPSS_MAJOR_VERSION >= 8
int
Hpss_OpendirHandle(
    const ns_ObjHandle_t        *  DirHandle,
    const sec_cred_t            *  Ucred);
#endif

signed32
Hpss_ParseAuthString(
    char                        *  AuthenticatorString,
    hpss_authn_mech_t           *  AuthnMechanism,
    hpss_rpc_auth_type_t        *  AuthenticatorType,
    void                        ** Authenticator);

int
Hpss_PIOEnd(
    hpss_pio_grp_t                 StripeGroup);

int
Hpss_PIOExecute(
    int                            Fd,
    uint64_t                       FileOffset,
    uint64_t                       Size,
    const hpss_pio_grp_t           StripeGroup,
    hpss_pio_gapinfo_t          *  GapInfo,
    uint64_t                    *  BytesMoved);

int
Hpss_PIOExportGrp(
    const hpss_pio_grp_t           StripeGroup,
    void                        ** Buffer,
    unsigned int                *  BufLength);

int
Hpss_PIOImportGrp(
    const void                  *  Buffer,
    unsigned int                   BufLength,
    hpss_pio_grp_t              *  StripeGroup);

int
Hpss_PIORegister(
    uint32_t                       StripeElement,
    const hpss_sockaddr_t       *  DataNetSockAddr,
    void                        *  DataBuffer,
    uint32_t                       DataBufLen,
    hpss_pio_grp_t                 StripeGroup,
    const hpss_pio_cb_t            IOCallback,
    const void                  *  IOCallbackArg);

int
Hpss_PIOStart(
    hpss_pio_params_t           *  InputParams,
    hpss_pio_grp_t              *  StripeGroup);

#if HPSS_MAJOR_VERSION >= 8
int
Hpss_ReadAttrsPlus(
    int                            Dirdes,
    uint64_t                       OffsetIn,
    uint32_t                       BufferSize,
    hpss_readdir_flags_t           Flags,
    uint32_t                    *  End,
    uint64_t                    *  OffsetOut,
    ns_DirEntry_t               *  DirentPtr);
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
    ns_DirEntry_t               *  DirentPtr);
#endif

int
Hpss_Readlink(
    const char                  *  Path,
    char                        *  Contents,
    size_t                         BufferSize);

int
Hpss_ReadlinkHandle(
    const ns_ObjHandle_t        *  ObjHandle,
    const char                  *  Path,
    char                        *  Contents,
    size_t                         BufferSize,
    const sec_cred_t            *  Ucred);

int
Hpss_Rename(
    const char                  *  Old,
    const char                  *  New);

int
Hpss_Rmdir(
    const char                  *  Path);

int
Hpss_SetConfiguration(
    const api_config_t          *  ConfigIn);

int
Hpss_SetCOSByHints(
    int                            Fildes,
    uint32_t                       Flags,
    const hpss_cos_hints_t      *  HintsPtr,
    const hpss_cos_priorities_t *  PrioPtr,
    hpss_cos_md_t               *  COSPtr);

int
Hpss_SetLoginCred(
    char                        *  PrincipalName,
    hpss_authn_mech_t              Mechanism,
    hpss_rpc_cred_type_t           CredType,
    hpss_rpc_auth_type_t           AuthType,
    void                        *  Authenticator);

int
Hpss_StageCallBack(
    const char                  *  Path,
    uint64_t                       Offset,
    uint64_t                       Length,
    uint32_t                       StorageLevel,
    bfs_callback_addr_t         *  CallBackPtr,
    uint32_t                       Flags,
    hpss_reqid_t                *  ReqID,
    bfs_bitfile_obj_handle_t    *  BitfileObj);

int
Hpss_Stat(
    const char                  *  Path,
    hpss_stat_t                 *  Buf);

int
Hpss_Symlink(
    const char                  *  Contents,
    const char                  *  Path);

int
Hpss_Truncate(
    const char                  *  Path,
    uint64_t                       Length);

mode_t
Hpss_Umask(mode_t CMask);

int
Hpss_Unlink(
    const char                  *  Path);

int
Hpss_UnlinkHandle(
    const ns_ObjHandle_t        *  ObjHandle,
    const char                  *  Path,
    const sec_cred_t            *  Ucred);

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION < 4)
int
Hpss_UserAttrGetAttrs(
    char                        *  Path,
    hpss_userattr_list_t        *  Attr,
    int                            XMLFlag);
#else
int
Hpss_UserAttrGetAttrs(
    const char                  *  Path,
    hpss_userattr_list_t        *  Attr,
    int                            XMLFlag,
    int                            XMLSize);
#endif

int
Hpss_UserAttrSetAttrs(
    const char                  *  Path,
    const hpss_userattr_list_t  *  Attr,
    const char                  *  Schema);

int
Hpss_Utime(
    const char                  *  Path,
    const struct utimbuf        *  Times);

#endif /* _HPSS_H_ */

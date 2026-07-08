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

/*
 * Local includes
 */
#include "hpss_error.h"

/*
 * A note about return values from our HPSS funcions.
 *
 * We return the same HPSS success codes (>= 0) that is typical of the HPSS
 * API. The difference is with error values. Typically, HPSS API will return
 * -<posix_error>. Instead, we return our own negative value which the caller
 *  can exchange to get <-posix_error> _and_ the last errno.
 *
 *  See hpss_error.h for more details.
 */

void
HpssAPI_ConvertTimeToPosixTime(
    const hpss_Attrs_t          *  Attrs,  // IN
    timestamp_sec_t             *  Atime,  // OUT
    timestamp_sec_t             *  Mtime,  // OUT
    timestamp_sec_t             *  Ctime); // OUT

signed32
Hpss_AuthnMechTypeFromString(
   const char                   *  AuthnMechString, // IN
   hpss_authn_mech_t            *  AuthnMech);      // OUT

/* Caller must free returned string */
char *
Hpss_BuildLevelString(void);

int
Hpss_Chmod(
    const char                  *  Path,  // IN
    mode_t                         Mode); // IN

char *
Hpss_ChompXMLHeader(
    char                        *  XML,     // IN
    char                        *  Header); // OUT

int
Hpss_Close(int Fildes); // IN

int
Hpss_Closedir(int Dirdes); // IN

int
Hpss_FileGetAttributes(
    const char                  *  Path,     // IN
    hpss_fileattr_t             *  AttrOut); // OUT

int
Hpss_FileGetXAttributes(
    const char                  *  Path,         // INT
    uint32_t                       Flags,        // INT
    uint32_t                       StorageLevel, // INT
    hpss_xfileattr_t            *  AttrOut);     // OUT

int
Hpss_FileGetXAttributesHandle(
    const ns_ObjHandle_t        *  ObjHandle,    // IN
    const char                  *  Path,         // IN
    const sec_cred_t            *  Ucred,        // IN
    uint32_t                       Flags,        // IN
    uint32_t                       StorageLevel, // IN
    hpss_xfileattr_t            *  AttrOut);     // OUT

int
Hpss_FilesetGetAttributes(
    const char                  *  Name,            // IN
    const uint64_t              *  FilesetId,       // IN
    const ns_ObjHandle_t        *  FilesetHandle,   // IN
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    const hpss_uuid_t           *  CoreServerUUID,  // IN
#else
    const hpss_srvr_id_t        *  CoreServerID,    // IN
#endif
    ns_FilesetAttrBits_t           FilesetAttrBits, // IN
    ns_FilesetAttrs_t           *  FilesetAttrs);   // OUT

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
int
Hpss_GetAsynchStatus(
    signed32                       CallBackId,
    hpssoid_t                   *  BitfileID,
    signed32                    *  Status);
#else
int
Hpss_GetAsyncStatus(
    hpss_reqid_t                   CallBackId, // IN
    bfs_bitfile_obj_handle_t    *  BitfileObj, // IN (incorrect in hpss_api.h)
    int32_t                     *  Status);    // OUT
#endif

char *
Hpss_Getenv(const char *Env); // IN

int
Hpss_GetConfiguration(
    api_config_t                *  ConfigOut); // OUT

int
Hpss_GetThreadUcred(
    sec_cred_t                  *  RetUcred); // OUT

int
Hpss_LoadDefaultThreadState(
    uid_t                          UserID,          // IN
    mode_t                         Umask,           // IN
    char                        *  ClientFullName); // IN

int
Hpss_Lstat(
    const char                  *  Path, // IN
    hpss_stat_t                 *  Buf); // OUT

int
Hpss_Mkdir(
    const char                  *  Path,  // IN
    mode_t                         Mode); // IN

int
Hpss_net_getaddrinfo(
    const char                  *  hostname,   // IN
    const char                  *  service,    // IN
    int                            flags,      // IN
    hpss_ipproto_t                 protocol,   // IN
    hpss_sockaddr_t             *  addr,       // OUT
    char                        *  errbuf,     // OUT
    size_t                         errbuflen); // IN

int
Hpss_Open(
    const char                  *  Path,      // IN
    int                            Oflag,     // IN
    mode_t                         Mode,      // IN
    const hpss_cos_hints_t      *  HintsIn,   // IN
    const hpss_cos_priorities_t *  HintsPri,  // IN
    hpss_cos_hints_t            *  HintsOut); // OUT

#if HPSS_MAJOR_VERSION >= 8
int
Hpss_OpendirHandle(
    const ns_ObjHandle_t        *  DirHandle, // IN
    const sec_cred_t            *  Ucred);    // IN
#endif

signed32
Hpss_ParseAuthString(
    char                        *  AuthenticatorString, // IN
    hpss_authn_mech_t           *  AuthnMechanism,      // IN
    hpss_rpc_auth_type_t        *  AuthenticatorType,   // OUT
    void                        ** Authenticator);      // OUT

int
Hpss_PIOEnd(
    hpss_pio_grp_t                 StripeGroup); // IN

int
Hpss_PIOExecute(
    int                            Fd,          // IN
    uint64_t                       FileOffset,  // IN
    uint64_t                       Size,        // IN
    const hpss_pio_grp_t           StripeGroup, // IN
    hpss_pio_gapinfo_t          *  GapInfo,     // OUT
    uint64_t                    *  BytesMoved); // OUT

int
Hpss_PIOExportGrp(
    const hpss_pio_grp_t           StripeGroup, // IN
    void                        ** Buffer,      // OUT
    unsigned int                *  BufLength);  // IN

int
Hpss_PIOImportGrp(
    const void                  *  Buffer,       // IN
    unsigned int                   BufLength,    // IN
    hpss_pio_grp_t              *  StripeGroup); // OUT

int
Hpss_PIORegister(
    uint32_t                       StripeElement,   // IN
    const hpss_sockaddr_t       *  DataNetSockAddr, // IN
    void                        *  DataBuffer,      // IN
    uint32_t                       DataBufLen,      // IN
    hpss_pio_grp_t                 StripeGroup,     // IN
    const hpss_pio_cb_t            IOCallback,      // IN
    const void                  *  IOCallbackArg);  // IN

int
Hpss_PIOStart(
    hpss_pio_params_t           *  InputParams,  // IN/OUT
    hpss_pio_grp_t              *  StripeGroup); // OUT

#if HPSS_MAJOR_VERSION >= 8
int
Hpss_ReadAttrsPlus(
    int                            Dirdes,     // IN
    uint64_t                       OffsetIn,   // IN
    uint32_t                       BufferSize, // IN
    hpss_readdir_flags_t           Flags,      // IN
    uint32_t                    *  End,        // OUT
    uint64_t                    *  OffsetOut,  // OUT
    ns_DirEntry_t               *  DirentPtr); // OUT
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
    ns_DirEntry_t               *  DirentPtr);    // OUT
#endif

int
Hpss_Readlink(
    const char                  *  Path,        // IN
    char                        *  Contents,    // OUT
    size_t                         BufferSize); // IN

int
Hpss_ReadlinkHandle(
    const ns_ObjHandle_t        *  ObjHandle,  // IN
    const char                  *  Path,       // IN
    char                        *  Contents,   // OUT
    size_t                         BufferSize, // IN
    const sec_cred_t            *  Ucred);     // IN

int
Hpss_Rename(
    const char                  *  Old,  // IN
    const char                  *  New); // IN

int
Hpss_Rmdir(
    const char                  *  Path); // IN

int
Hpss_SetConfiguration(
    const api_config_t          *  ConfigIn); // IN

int
Hpss_SetCOSByHints(
    int                            Fildes,   // IN
    uint32_t                       Flags,    // IN
    const hpss_cos_hints_t      *  HintsPtr, // IN
    const hpss_cos_priorities_t *  PrioPtr,  // IN
    hpss_cos_md_t               *  COSPtr);  // OUT

int
Hpss_SetLoginCred(
    char                        *  PrincipalName,  // IN
    hpss_authn_mech_t              Mechanism,      // IN
    hpss_rpc_cred_type_t           CredType,       // IN
    hpss_rpc_auth_type_t           AuthType,       // IN
    void                        *  Authenticator); // IN

int
Hpss_StageCallBack(
    const char                  *  Path,         // IN
    uint64_t                       Offset,       // IN
    uint64_t                       Length,       // IN
    uint32_t                       StorageLevel, // IN
    bfs_callback_addr_t         *  CallBackPtr,  // IN
    uint32_t                       Flags,        // IN
    hpss_reqid_t                *  ReqID,        // OUT
#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
    hpssoid_t                   *  BitfileID);   // OUT
#else
    bfs_bitfile_obj_handle_t    *  BitfileObj);  // OUT
#endif

int
Hpss_Stat(
    const char                  *  Path, // IN
    hpss_stat_t                 *  Buf); // OUT

int
Hpss_Symlink(
    const char                  *  Contents, // IN
    const char                  *  Path);    // IN

int
Hpss_Truncate(
    const char                  *  Path,    // IN
    uint64_t                       Length); // IN

mode_t
Hpss_Umask(mode_t CMask); // IN

int
Hpss_Unlink(
    const char                  *  Path); // IN

int
Hpss_UnlinkHandle(
    const ns_ObjHandle_t        *  ObjHandle, // IN
    const char                  *  Path,      // IN
    const sec_cred_t            *  Ucred);    // IN

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION <= 4)
int
Hpss_UserAttrGetAttrs(
    char                        *  Path,
    hpss_userattr_list_t        *  Attr,
    int                            XMLFlag);
#else
int
Hpss_UserAttrGetAttrs(
    const char                  *  Path,     // IN
    hpss_userattr_list_t        *  Attr,     // IN/OUT
    int                            XMLFlag,  // IN
    int                            XMLSize); // IN
#endif

int
Hpss_UserAttrSetAttrs(
    const char                  *  Path,    // IN
    const hpss_userattr_list_t  *  Attr,    // IN
    const char                  *  Schema); // IN

int
Hpss_Utime(
    const char                  *  Path,   // IN
    const struct utimbuf        *  Times); // IN

#if (HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9
int
HpssAPI_StageBatchInit(
    hpss_stage_batch_t          * Batch, // IN
    int                           Len);  // IN

void
HpssAPI_StageBatchFree(
   hpss_stage_batch_t           * Batch); // IN

int
HpssAPI_StageBatchInsertBFObj(
    hpss_stage_batch_t             * Batch,            // IN/OUT
    int                              Idx,              // IN
    const bfs_bitfile_obj_handle_t * BfObj,            // IN
    int                              FromStorageLevel, // IN
    int                              ToStorageLevel,   // IN
    u_signed64                       Offset,           // IN
    u_signed64                       Length,           // IN
    uint32_t                         Flags);           // IN

int
Hpss_StageBatchCallBack(
    hpss_stage_batch_t          *  Batch,       // IN
    bfs_callback_addr_t         *  CallBackPtr, // IN
    hpss_reqid_t                *  ReqID,       // OUT
    hpss_stage_bitfile_list_t   *  BFIDs,       // OUT
    hpss_stage_batch_status_t   *  Status);     // OUT

void
HpssAPI_StageStatusFree(
    hpss_stage_batch_status_t   *  Status); // IN

#endif // (HPSS_MAJOR_VERSION == 9 && HPSS_MINOR_VERSION >= 3) || HPSS_MAJOR_VERSION > 9

#endif /* _HPSS_H_ */

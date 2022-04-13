#ifndef _HPSS_LOG_H_
#define _HPSS_LOG_H_

#include <hpss_api.h>
#include <hpss_version.h>

/*
 * Local includes.
 */
#include "logging.h"

#define ACCT_REC_T(a) UNSIGNED(a)

#define API_CONFIG_T_PTR(p) _api_config_t_ptr(pool, p)
char *
_api_config_t_ptr(struct pool * pool, const api_config_t * p);

#define BF_SC_ATTRIB_T(a) _bf_sc_attrib_t(pool, a)
char *
_bf_sc_attrib_t(struct pool * pool, bf_sc_attrib_t a);

#define BF_VV_ATTRIB_T(a) _bf_vv_attrib_t(pool, a)
char *
_bf_vv_attrib_t(struct pool * pool, bf_vv_attrib_t a);

#if (HPSS_MAJOR_VERSION >= 8 || HPSS_MINOR_VERSION > 4)
 #define BFS_BITFILE_OBJ_HANDLE_T(h) _bfs_bitfile_obj_handle_t(pool, h)
 char *
 _bfs_bitfile_obj_handle_t(struct pool * pool, bfs_bitfile_obj_handle_t h);

 #define BFS_BITFILE_OBJ_HANDLE_T_PTR(p) _bfs_bitfile_obj_handle_t_ptr(pool, p)
 char *
 _bfs_bitfile_obj_handle_t_ptr(struct pool * pool,
                               const bfs_bitfile_obj_handle_t * p);
#endif

#define BYTES_PTR(p) UNSIGNED_CHAR_PTR(p)

#define HPSS_ATTRS_T(a) _hpss_attrs_t(pool, a) 
char *
_hpss_attrs_t(struct pool * pool, hpss_Attrs_t a);

#define HPSS_ATTRS_T_PTR(p) _hpss_attrs_t_ptr(pool, p) 
char *
_hpss_attrs_t_ptr(struct pool * pool, const hpss_Attrs_t * p);

// TODO: We could print the string that corresponds to the enum
#define HPSS_AUTHN_MECH_T(m) INT(m)

#define HPSS_AUTHN_MECH_T_PTR(p) _hpss_authn_mech_t_ptr(pool, p)
char *
_hpss_authn_mech_t_ptr(struct pool * pool, const hpss_authn_mech_t * p);

#define HPSS_COS_HINTS_T_PTR(p) _hpss_cos_hints_t_ptr(pool, p)
char *
_hpss_cos_hints_t_ptr(struct pool * pool, const hpss_cos_hints_t * p);

#define HPSS_COS_MD_T_PTR(p) _hpss_cos_md_t_ptr(pool, p)
char *
_hpss_cos_md_t_ptr(struct pool * pool, const hpss_cos_md_t * p);

#define HPSS_COS_PRIORITIES_T_PTR(p) _hpss_cos_priorities_t_ptr(pool, p)
char *
_hpss_cos_priorities_t_ptr(struct pool * pool, const hpss_cos_priorities_t * p);

#define HPSS_DISTRIBUTIONKEY_T(k) UNSIGNED16(k)

#define HPSS_ERRNO_STATE_T(e) _hpss_errno_state_t(pool, e)
char *
_hpss_errno_state_t(struct pool * pool, hpss_errno_state_t errno_state);

#define HPSS_FILEATTR_T(a) _hpss_fileattr_t(pool, a)
char *
_hpss_fileattr_t(struct pool * pool, const hpss_fileattr_t *);

#define HPSS_RPC_AUTH_TYPE_T(t) INT(t)
#define HPSS_RPC_CRED_TYPE_T(t) INT(t)

#define HPSS_HASH_TYPE_T(t) INT(t)

#define HPSS_PIO_GAPINFO_T_PTR(p) _hpss_pio_gapinfo_t_ptr(pool, p)
char *
_hpss_pio_gapinfo_t_ptr(struct pool * pool, const hpss_pio_gapinfo_t * p);

#define HPSS_PIO_OPERATION_T(o) INT(o)

#define HPSS_PIO_OPTIONS_T(o) HEX(o)

#define HPSS_PIO_PRARAMS_T_PTR(p) _hpss_pio_prarams_t_ptr(pool, p)
char *
_hpss_pio_prarams_t_ptr(struct pool * pool, const hpss_pio_params_t * p);

#define HPSS_PIO_TRANSPORT_T(t) HEX(t)

#define HPSS_READDIR_FLAGS_T(f) HEX(f)

#if HPSS_MAJOR_VERSION < 8
 #define HPSS_REQID_T(r) UNSIGNED(r)
#else
 #define HPSS_REQID_T(r) HPSS_UUID_T(r)
#endif

#define HPSS_REQID_T_PTR(p) _hpss_reqid_t_ptr(pool, p)
char *
_hpss_reqid_t_ptr(struct pool * pool, const hpss_reqid_t * p);

// TODO: We could print the string that corresponds to the enum
#define HPSS_RPC_PROT_LEVEL_T(m) INT(m)

// TODO: We could print the string that corresponds to the enum
#define HPSS_RPC_AUTH_TYPE_T_PTR(p) _hpss_rpc_auth_type_t_ptr(pool, p)
char *
_hpss_rpc_auth_type_t_ptr(struct pool * pool, const hpss_rpc_auth_type_t * p);

#define HPSS_TRASHRECORD_T(t) _hpss_trashrecord_t(pool, t)
char *
_hpss_trashrecord_t(struct pool * pool, hpss_TrashRecord_t);

#define HPSS_UUID_T(u) _hpss_uuid_t(pool, u)
char *
_hpss_uuid_t(struct pool * pool, hpss_uuid_t u);

#define HPSS_UUID_T_PTR(p) _hpss_uuid_t_ptr(pool, p)
char *
_hpss_uuid_t_ptr(struct pool * pool, const hpss_uuid_t * ptr);

#define HPSS_XFILEATTR_T_PTR(x) _hpss_xfileattr_t_ptr(pool, x)
char *
_hpss_xfileattr_t_ptr(struct pool * pool, const hpss_xfileattr_t * x);


#if (HPSS_MAJOR_VERSION >= 8 || HPSS_MINOR_VERSION > 4)
 #define HPSS_SRVR_ID_T(i) UNSIGNED(i)

 #define HPSS_SRVR_ID_T_PTR(p) _hpss_srvr_id_t_ptr(pool, p)
 char *
 _hpss_srvr_id_t_ptr(struct pool * pool, const hpss_srvr_id_t * p);
#endif



#define HPSS_STAT_T_PTR(p) _hpss_stat_t_ptr(pool, p)
char *
_hpss_stat_t_ptr(struct pool * pool, const hpss_stat_t * p);

#define HPSS_USERATTR_T(a) _hpss_userattr_t(pool, a)
char *
_hpss_userattr_t(struct pool * pool, hpss_userattr_t a);

#define HPSS_USERATTR_LIST_T_PTR(p) _hpss_userattr_list_t_ptr(pool, p)
char *
_hpss_userattr_list_t_ptr(struct pool * pool, const hpss_userattr_list_t * p);

#define HPSSOID_T(o) _hpssoid_t(pool, o)
char *
_hpssoid_t(struct pool * pool, hpssoid_t o);

#define HPSSOID_T_PTR(p) _hpssoid_t_ptr(pool, p)
char *
_hpssoid_t_ptr(struct pool * pool, const hpssoid_t * p);

#define NS_DIRENTRY_T(e) _ns_direntry_t(pool, e)
char *
_ns_direntry_t(struct pool * pool, ns_DirEntry_t e);

#define NS_DIRENTRY_T_ARRAY(a, c) _ns_direntry_t_array(pool, a, c)
char *
_ns_direntry_t_array(struct pool * pool, const ns_DirEntry_t * a, size_t cnt);

#define NS_FILESETATTRBITS_T(b) HEX64(b)

#define NS_FILESETATTRS_T(p) _ns_filesetattrs_t_ptr(pool, p)
char *
_ns_filesetattrs_t_ptr(struct pool * pool, const ns_FilesetAttrs_t * p);

#define NS_OBJHANDLE_T(o) _ns_objhandle_t(pool, o) 
char *
_ns_objhandle_t(struct pool * pool, ns_ObjHandle_t o);

#define NS_OBJHANDLE_T_PTR(p) _ns_objhandle_t_ptr(pool, p) 
char *
_ns_objhandle_t_ptr(struct pool * pool, const ns_ObjHandle_t * p);

#define PV_LIST_ELEMENT_T(l) _pv_list_element_t(pool, l)
char *
_pv_list_element_t(struct pool * pool, pv_list_element_t e);

#define PV_LIST_T(l) _pv_list_t_ptr(pool, l)
char *
_pv_list_t_ptr(struct pool * pool, const pv_list_t * l);

#define SEC_CRED_T_PTR(p) _sec_cred_t_ptr(pool, p)
char *
_sec_cred_t_ptr(struct pool * pool, const sec_cred_t * p);

#define TIMESTAMP_SEC_T(t) UNSIGNED(t)

#define TIMESTAMP_SEC_T_PTR(p) _timestamp_sec_t_ptr(pool, p)
char *
_timestamp_sec_t_ptr(struct pool * pool, const timestamp_sec_t * p);

#endif /* _HPSS_LOG_H_ */

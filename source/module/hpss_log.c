#if HPSS_MAJOR_VERSION >= 8
#include <hpss_RequestID.h>
#endif
#include <hpss_types.h>

#include "hpss_log.h"
#include "strings.h"

char *
_api_config_t_ptr(struct pool * pool, const api_config_t * p)
{
    if (p == NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "Flags=%s, "                // unsigned int
            "DebugValue=%s, "           // int
            "TransferType=%s, "         // int
            "NumRetries=%s, "           // int
            "BusyDelay=%s, "            // int
            "TotalDelay=%s, "           // int
            "LimitedRetries=%s, "       // int
            "MaxConnections=%s, "       // int
            "ReuseDataConnections=%s, " // int
            "UsePortRange=%s, "         // int
            "RetryStageInp=%s, "        // int
            "DMAPWriteUpdates=%s, "     // int
            "AuthnMech=%s, "            // hpss_authn_mech_t
            "RPCProtLevel=%s, "         // hpss_rpc_prot_level_t
            "DescName=%s, "             // char[HPSS_MAX_DESC_NAME]
            "DebugPath=%s, "            // char[HPSS_MAX_DESC_NAME]
            "HostName=%s, "             // char[HPSS_MAX_DESC_NAME]
            "XMLSize=%s,"               // signed32
        "}",
            UNSIGNED(p->Flags),
            INT(p->DebugValue),
            INT(p->TransferType),
            INT(p->NumRetries),
            INT(p->BusyDelay),
            INT(p->TotalDelay),
            INT(p->LimitedRetries),
            INT(p->MaxConnections),
            INT(p->ReuseDataConnections),
            INT(p->UsePortRange),
            INT(p->RetryStageInp),
            INT(p->DMAPWriteUpdates),
            HPSS_AUTHN_MECH_T(p->AuthnMech),
            HPSS_RPC_PROT_LEVEL_T(p->RPCProtLevel),
            CHAR_PTR(p->DescName),
            CHAR_PTR(p->DebugPath),
            CHAR_PTR(p->HostName),
            INT(p->XMLSize));
}

char *
_bf_sc_attrib_t(struct pool * pool, bf_sc_attrib_t a)
{
    return _sprintf(
        pool,
        "{"
            "VVAttrib={"             // bf_vv_attrib_t
                "%s, "                 // 1
                "%s, "                 // 2
                "%s, "                 // 3
                "%s, "                 // 4
                "%s, "                 // 5
                "%s, "                 // 6
                "%s, "                 // 7
                "%s, "                 // 8
                "%s, "                 // 9
                "%s"                   // 10 BFS_MAX_VV_TO_RETURN_AT_LEVEL
            "}, "
            "NumberOfVVs=%s, "       // unsigned32
            "BytesAtLevel=%s, "      // u_signed64
            "OptimumAccessSize=%s, " // unsigned32
            "StripeWidth=%s, "       // unsigned32
            "StripeLength=%s, "      // u_signed64
            "Flags=%s"               // unsigned32
        "}",
            BF_VV_ATTRIB_T(a.VVAttrib[0]),
            BF_VV_ATTRIB_T(a.VVAttrib[1]),
            BF_VV_ATTRIB_T(a.VVAttrib[2]),
            BF_VV_ATTRIB_T(a.VVAttrib[3]),
            BF_VV_ATTRIB_T(a.VVAttrib[4]),
            BF_VV_ATTRIB_T(a.VVAttrib[5]),
            BF_VV_ATTRIB_T(a.VVAttrib[6]),
            BF_VV_ATTRIB_T(a.VVAttrib[7]),
            BF_VV_ATTRIB_T(a.VVAttrib[8]),
            BF_VV_ATTRIB_T(a.VVAttrib[9]),
            UNSIGNED(a.NumberOfVVs),
            UNSIGNED64(a.BytesAtLevel),
            UNSIGNED(a.OptimumAccessSize),
            UNSIGNED(a.StripeWidth),
            UNSIGNED64(a.StripeLength),
            HEX(a.Flags));
}

char *
_bf_vv_attrib_t(struct pool * pool, bf_vv_attrib_t a)
{
    return _sprintf(
        pool,
        "{"
             "VVID=%s, "              // hpssoid_t
             "RelPosition=%s, "       // signed32
             "RelPositionOffset=%s, " // u_signed64
             "BytesOnVV=%s, "         // u_signed64
             "PVList=%s, "            // pv_list_t *
        "}",
            HPSSOID_T(a.VVID),
            SIGNED(a.RelPosition),
            UNSIGNED64(a.RelPositionOffset),
            UNSIGNED64(a.BytesOnVV),
            PV_LIST_T(a.PVList));
}

char *
_bfs_bitfile_obj_handle_t(struct pool * pool, bfs_bitfile_obj_handle_t h)
{
    return _bfs_bitfile_obj_handle_t_ptr(pool, &h);
}

char *
_bfs_bitfile_obj_handle_t_ptr(struct pool * pool,
                               const bfs_bitfile_obj_handle_t * p)
{
    if (p == NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "BfId=%s, "    // hpssoid_t
            "BfHash=%s, "  // hpss_distributionkey_t
            "ValidHash=%s" // uint32_t
        "}",
        HPSSOID_T(p->BfId),
        HPSS_DISTRIBUTIONKEY_T(p->BfHash),
        UNSIGNED(p->ValidHash));
}

char *
_hpss_attrs_t(struct pool * pool, hpss_Attrs_t a)
{
    return _hpss_attrs_t_ptr(pool, &a);
}

char *
_hpss_attrs_t_ptr(struct pool * pool, const hpss_Attrs_t * p)
{
    if (p == NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "Account=%s, "             // acct_rec_t
            "BitfileObj=%s, "          // bfs_bitfile_obj_handle_t
            "Comment=%s, "             // char *
            "CompositePerms=%s, "      // uint32_t
            "COSId=%s, "               // uint32_t
            "DataLength=%s, "          // uint64_t
            "EntryCount=%s, "          // uint32_t
            "ExtendedACLs=%s, "        // uint32_t
            "FamilyId=%s, "            // uint32_t
            "FilesetHandle=%s, "       // ns_ObjHandle_t
            "FilesetId=%s, "           // uint64_t
            "FilesetRootObjectId=%s, " // uint64_t
            "FilesetStateFlags=%s, "   // uint32_t
            "FilesetType=%s, "         // uint32_t
            "GID=%s, "                 // uint32_t
            "GroupPerms=%s, "          // uint32_t
            "LinkCount=%s, "           // uint32_t
            "ModePerms=%s, "           // uint32_t
            "OpenCount=%s, "           // uint32_t
            "OptionFlags=%s, "         // uint32_t
            "OtherPerms=%s, "          // uint32_t
            "ReadCount=%s, "           // uint32_t
            "RealmId=%s, "             // uint32_t
            "RegisterBitMap=%s, "      // uint64_t
            "SubSystemId=%s, "         // uint32_t
            "TimeCreated=%s, "         // timestamp_sec_t
            "TimeLastRead=%s, "        // timestamp_sec_t
            "TimeLastWritten=%s, "     // timestamp_sec_t
            "TimeModified=%s, "        // timestamp_sec_t
            "TrashInfo=%s, "           // hpss_TrashRecord_t
            "Type=%s, "                // uint32_t
            "UID=%s, "                 // uint32_t
            "UserPerms=%s, "           // uint32_t
            "WriteCount=%s"            // uint32_t
        "}",
            ACCT_REC_T(p->Account),
            BFS_BITFILE_OBJ_HANDLE_T(p->BitfileObj),
            CHAR_PTR(p->Comment),
            HEX(p->CompositePerms),
            UNSIGNED(p->COSId),
            UNSIGNED64(p->DataLength),
            UNSIGNED(p->EntryCount),
            UNSIGNED(p->ExtendedACLs),
            UNSIGNED(p->FamilyId),
            NS_OBJHANDLE_T(p->FilesetHandle),
            UNSIGNED64(p->FilesetId),
            UNSIGNED64(p->FilesetRootObjectId),
            UNSIGNED(p->FilesetStateFlags),
            UNSIGNED(p->FilesetType),
            UNSIGNED(p->GID),
            HEX(p->GroupPerms),
            UNSIGNED(p->LinkCount),
            HEX(p->ModePerms),
            UNSIGNED(p->OpenCount),
            UNSIGNED(p->OptionFlags),
            HEX(p->OtherPerms),
            UNSIGNED(p->ReadCount),
            UNSIGNED(p->RealmId),
            UNSIGNED64(p->RegisterBitMap),
            UNSIGNED(p->SubSystemId),
            TIMESTAMP_SEC_T(p->TimeCreated),
            TIMESTAMP_SEC_T(p->TimeLastRead),
            TIMESTAMP_SEC_T(p->TimeLastWritten),
            TIMESTAMP_SEC_T(p->TimeModified),
            HPSS_TRASHRECORD_T(p->TrashInfo),
            UNSIGNED(p->Type),
            UNSIGNED(p->UID),
            HEX(p->UserPerms),
            UNSIGNED(p->WriteCount));
}

char *
_hpss_authn_mech_t_ptr(struct pool * pool, const hpss_authn_mech_t * p)
{
    if (p == NULL)
        return PTR(p);
    return INT(*p);
}

char *
_hpss_cos_hints_t_ptr(struct pool * pool, const hpss_cos_hints_t * p)
{
    if (p == NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "COSId=%s, "             // unsigned32
            "COSName=%s, "           // char[HPSS_MAX_OBJECT_NAME]
            "Flags=%s, "             // unsigned32
            "OptimumAccessSize=%s, " // u_signed64
            "MinFileSize=%s, "       // u_signed64
            "MaxFileSize=%s, "       // u_signed64
            "AccessFrequency=%s, "   // unsigned32
            "TransferRate=%s, "      // unsigned32
            "AvgLatency=%s, "        // unsigned32
            "WriteOps=%s, "          // unsigned32
            "ReadOps=%s, "           // unsigned32
            "StageCode=%s, "         // unsigned32
            "StripeWidth=%s, "       // unsigned32
            "StripeLength=%s, "      // u_signed64
            "FamilyId=%s",           // unsigned32
        "}",
            UNSIGNED(p->COSId),
            CHAR_PTR(p->COSName),
            HEX(p->Flags),
            UNSIGNED64(p->OptimumAccessSize),
            UNSIGNED64(p->MinFileSize),
            UNSIGNED64(p->MaxFileSize),
            UNSIGNED(p->AccessFrequency),
            UNSIGNED(p->TransferRate),
            UNSIGNED(p->AvgLatency),
            UNSIGNED(p->WriteOps),
            UNSIGNED(p->ReadOps),
            UNSIGNED(p->StageCode),
            UNSIGNED(p->StripeWidth),
            UNSIGNED64(p->StripeLength),
            UNSIGNED(p->FamilyId));
}

char *
_hpss_cos_md_t_ptr(struct pool * pool, const hpss_cos_md_t * p)
{
    if (p == NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "COSId=%s, "             // unsigned32
            "HierId=%s, "            // unsigned32
            "COSName=%s, "           // char[HPSS_MAX_OBJECT_NAME]
            "OptimumAccessSize=%s, " // unsigned32
            "Flags=%s, "             // unsigned32
            "MinFileSize=%s, "       // u_signed64
            "MaxFileSize=%s, "       // u_signed64
            "AccessFrequency=%s, "   // unsigned32
            "TransferRate=%s, "      // unsigned32
            "AvgLatency=%s, "        // unsigned32
            "WriteOps=%s, "          // unsigned32
            "ReadOps=%s, "           // unsigned32
            "StageCode=%s, "         // unsigned32
            "AllocMethod=%s, "       // unsigned32
            "FileHashType=%s"        // hpss_hash_type_t
        "}",
            UNSIGNED(p->COSId),
            UNSIGNED(p->HierId),
            CHAR_PTR(p->COSName),
            UNSIGNED(p->OptimumAccessSize),
            HEX(p->Flags),
            UNSIGNED64(p->MinFileSize),
            UNSIGNED64(p->MaxFileSize),
            UNSIGNED(p->AccessFrequency),
            UNSIGNED(p->TransferRate),
            UNSIGNED(p->AvgLatency),
            UNSIGNED(p->WriteOps),
            UNSIGNED(p->ReadOps),
            UNSIGNED(p->StageCode),
            UNSIGNED(p->AllocMethod),
            HPSS_HASH_TYPE_T(p->FileHashType));
}

char *
_hpss_cos_priorities_t_ptr(struct pool * pool, const hpss_cos_priorities_t * p)
{
    if (p == NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "COSIdPriority=%s, "             // unsigned32
            "COSNamePriority=%s, "           // unsigned32
            "OptimumAccessSizePriority=%s, " // unsigned32
            "MinFileSizePriority=%s, "       // unsigned32
            "MaxFileSizePriority=%s, "       // unsigned32
            "AccessFrequencyPriority=%s, "   // unsigned32
            "TransferRatePriority=%s, "      // unsigned32
            "AvgLatencyPriority=%s, "        // unsigned32
            "WriteOpsPriority=%s, "          // unsigned32
            "ReadOpsPriority=%s, "           // unsigned32
            "StageCodePriority=%s, "         // unsigned32
            "StripeWidthPriority=%s, "       // unsigned32
            "StripeLengthPriority=%s, "      // unsigned32
            "FamilyIdPriority=%s, "          // unsigned32
        "}",
            UNSIGNED(p->COSIdPriority),
            UNSIGNED(p->COSNamePriority),
            UNSIGNED(p->OptimumAccessSizePriority),
            UNSIGNED(p->MinFileSizePriority),
            UNSIGNED(p->MaxFileSizePriority),
            UNSIGNED(p->AccessFrequencyPriority),
            UNSIGNED(p->TransferRatePriority),
            UNSIGNED(p->AvgLatencyPriority),
            UNSIGNED(p->WriteOpsPriority),
            UNSIGNED(p->ReadOpsPriority),
            UNSIGNED(p->StageCodePriority),
            UNSIGNED(p->StripeWidthPriority),
            UNSIGNED(p->StripeLengthPriority),
            UNSIGNED(p->FamilyIdPriority));
}

char *
_hpss_error_state_t(struct pool * pool, hpss_errno_state_t errno_state)
{
    return _sprintf(pool,
                    "{hpss_errno=%s, func=%s, requestId=%s}", 
                    INT(errno_state.hpss_errno),
                    CHAR_PTR(errno_state.func),
                    HPSS_REQID_T(errno_state.requestId));
}

char *
_hpss_fileattr_t(struct pool * pool, const hpss_fileattr_t * fileattr)
{
    return _sprintf(pool,
                    "{"
                        "ObjectHandle=%s, " // ns_ObjHandle_t
                        "Attrs=%s"          // hpss_Attrs_t
                    "}", 
                    NS_OBJHANDLE_T(fileattr->ObjectHandle),
                    HPSS_ATTRS_T(fileattr->Attrs));
}

char *
_hpss_pio_gapinfo_t_ptr(struct pool * pool, const hpss_pio_gapinfo_t * p)
{
    if (p == NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "Offset=%s, " // u_signed64
            "Length=%s"   // u_signed64
        "}",
            UNSIGNED64(p->Offset),
            UNSIGNED64(p->Length));
}

char *
_hpss_pio_prarams_t_ptr(struct pool * pool, const hpss_pio_params_t * p)
{
    if (p == NULL)
        return PTR(p);

   return _sprintf(
       pool,
       "{"
           "Operation=%s, "       // hpss_pio_operation_t
           "ClntStripeWidth=%s, " // unsigned32
           "BlockSize=%s, "       // unsigned32
           "FileStripeWidth=%s, " // unsigned32
           "IOTimeOutSecs=%s, "   // unsigned32
           "Transport=%s, "       // hpss_pio_transport_t
           "Options=%s"           // hpss_pio_options_t
       "}",
           HPSS_PIO_OPERATION_T(p->Operation),
           UNSIGNED(p->ClntStripeWidth),
           UNSIGNED(p->BlockSize),
           UNSIGNED(p->FileStripeWidth),
           UNSIGNED(p->IOTimeOutSecs),
           HPSS_PIO_TRANSPORT_T(p->Transport),
           HPSS_PIO_OPTIONS_T(p->Options));
}

#if HPSS_MAJOR_VERSION >= 8
char *
_hpss_reqid_t(struct pool * pool, hpss_reqid_t r)
{
    char * s1 = hpss_RequestIDtoString(&r);
    char * s2 = _sprintf(pool, "[%s] %s", s1, HPSS_UUID_T(r));
    free(s1);
    return s2;
}
#endif

char *
_hpss_reqid_t_ptr(struct pool * pool, const hpss_reqid_t * p)
{
    if (p == NULL)
        return PTR(p);
    return HPSS_REQID_T(*p);
}

char *
_hpss_rpc_auth_type_t_ptr(struct pool * pool, const hpss_rpc_auth_type_t * p)
{
    if (p == NULL)
        return PTR(p);
    return INT(*p);
}

char *
_hpss_trashrecord_t(struct pool * pool, hpss_TrashRecord_t t)
{
    return _sprintf(
        pool,
        "{"
            "ParentId=%s, "           // u_signed64
#if HPSS_MAJOR_VERSION >= 8
            "ParentNsHash=%s, "       // hpss_distributionkey_t
#endif
            "Handle=%s, "             // ns_ObjHandle_t
            "UID=%s, "                // unsigned32
            "RealmId=%s, "            // unsigned32
            "TimeDeleted=%s, "        // timestamp_sec_t
            "TimeCreated=%s, "        // timestamp_sec_t
            "TimeLastRead=%s, "       // timestamp_sec_t
            "TimeModified=%s, "       // timestamp_sec_t
            "LengthAtDeleteTime=%s, " // u_signed64
            "BitfileId=%s, "          // hpssoid_t
#if HPSS_MAJOR_VERSION >= 8
            "BitfileHash=%s, "        // hpss_distributionkey_t
#endif
            "Path=%s, "               // char[HPSS_MAX_TRASH_PATH]
            "Name=%s"                 // char[HPSS_MAX_TRASH_PATH]
        "}",
            UNSIGNED64(t.ParentId),
            HPSS_DISTRIBUTIONKEY_T(t.ParentNsHash),
            NS_OBJHANDLE_T(t.Handle),
            UNSIGNED(t.UID),
            UNSIGNED(t.RealmId),
            TIMESTAMP_SEC_T(t.TimeDeleted),
            TIMESTAMP_SEC_T(t.TimeCreated),
            TIMESTAMP_SEC_T(t.TimeLastRead),
            TIMESTAMP_SEC_T(t.TimeModified),
            UNSIGNED64(t.LengthAtDeleteTime),
            HPSSOID_T(t.BitfileId),
            HPSS_DISTRIBUTIONKEY_T(t.BitfileHash),
            CHAR_PTR(t.Path),
            CHAR_PTR(t.Name));
}

char *
_hpss_uuid_t(struct pool * pool, hpss_uuid_t u)
{
    return _hpss_uuid_t_ptr(pool, &u);
}

char *
_hpss_uuid_t_ptr(struct pool * pool, const hpss_uuid_t * p)
{
    if (p== NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "time_low=%s, "                  // uint32_t
            "time_mid=%s, "                  // uint16_t
            "time_hi_and_version=%s, "       // uint16_t
            "clock_seq_hi_and_reserved=%s, " // uint8_t
            "clock_seq_low=%s, "             // uint8_t
            "node=%s"                        // char[6]
        "}",
            UNSIGNED(p->time_low),
            UNSIGNED16(p->time_mid),
            UNSIGNED16(p->time_hi_and_version),
            UNSIGNED8(p->clock_seq_hi_and_reserved),
            UNSIGNED8(p->clock_seq_low),
            HEX8(p->node[0]),
            HEX8(p->node[1]),
            HEX8(p->node[2]),
            HEX8(p->node[3]),
            HEX8(p->node[4]),
            HEX8(p->node[5]));
}

char *
_hpss_xfileattr_t_ptr(struct pool * pool, const hpss_xfileattr_t * p)
{
    if (p == NULL)
    return PTR(p);

    return _sprintf(
        pool,
        "{"
            "ObjectHandle=%s, " // ns_ObjHandle_t
            "Attrs=%s, "        // hpss_Attrs_t
            "SCAttrib={"        // bf_sc_attrib_t[]
                "%s, "          // 1
                "%s, "          // 2
                "%s, "          // 3
                "%s, "          // 4
                "%s"            // 5 HPSS_MAX_STORAGE_LEVELS
            "}"
        "}",
            NS_OBJHANDLE_T(p->ObjectHandle),
            HPSS_ATTRS_T(p->Attrs),
            BF_SC_ATTRIB_T(p->SCAttrib[0]),
            BF_SC_ATTRIB_T(p->SCAttrib[1]),
            BF_SC_ATTRIB_T(p->SCAttrib[2]),
            BF_SC_ATTRIB_T(p->SCAttrib[3]),
            BF_SC_ATTRIB_T(p->SCAttrib[4])); // HPSS_MAX_STORAGE_LEVELS
}

char *
_hpss_srvr_id_t_ptr(struct pool * pool, const hpss_srvr_id_t * p)
{
    if (p == NULL)
        return PTR(p);
    return UNSIGNED(*p);
}

char *
_hpss_stat_t_ptr(struct pool * pool, const hpss_stat_t * p)
{
    if (p == NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "st_dev=%s, "        // unsigned32
            "st_ino=%s, "        // u_signed64
            "st_nlink=%s, "      // unsigned16
            "st_flag=%s, "       // unsigned16
            "st_uid=%s, "        // unsigned32
            "st_gid=%s, "        // unsigned32
            "st_rdev=%s, "       // unsigned32
            "st_ssize=%s, "      // u_signed64
            "hpss_st_atime=%s, " // timestamp_sec_t
            "hpss_st_mtime=%s, " // timestamp_sec_t
            "hpss_st_ctime=%s, " // timestamp_sec_t
            "st_blksize=%s, "    // unsiged32
            "st_blocks=%s, "     // unsiged32
            "st_vfstype=%s, "    // signed32
            "st_vfs=%s, "        // unsigned32
            "st_type=%s, "       // unsigned32
            "st_gen=%s, "        // unsigned32
            "st_size=%s, "       // u_signed64
            "st_mode=%s"         // unsigned32
        "}",
            UNSIGNED(p->st_dev),
            UNSIGNED64(p->st_ino),
            UNSIGNED16(p->st_nlink),
            UNSIGNED16(p->st_flag),
            UNSIGNED(p->st_uid),
            UNSIGNED(p->st_gid),
            UNSIGNED(p->st_rdev),
            UNSIGNED64(p->st_ssize),
            TIMESTAMP_SEC_T(p->hpss_st_atime),
            TIMESTAMP_SEC_T(p->hpss_st_mtime),
            TIMESTAMP_SEC_T(p->hpss_st_ctime),
            UNSIGNED(p->st_blksize),
            UNSIGNED(p->st_blocks),
            SIGNED(p->st_vfstype),
            HEX(p->st_vfs),
            HEX(p->st_type),
            UNSIGNED(p->st_gen),
            UNSIGNED64(p->st_size),
            HEX(p->st_mode));
}

// XXX which these always have valid inputs?
char *
_hpss_userattr_t(struct pool * pool, hpss_userattr_t a)
{
    return _sprintf(
        pool,
        "{"
            "Key=%s, "
            "Value=%s"
        "}",
        CHAR_PTR(a.Key),
        CHAR_PTR(a.Value));
}

// TODO: duplicate code of _pv_list_val
/*
 * It would be create to consolidate all of the array functions, but C/GCC is
 * not very helpful in this respect. If we consolidate the array function, we
 * get warnings for passing function prototypes that use (void *). GCC has
 * pragmas to deal with this, but not in the version of GCC we currently use.
 * So we'll punt until we can upgrade compilers or move to C++.
 */
static char *
_hpss_userattr_list_t_array(struct pool * pool, const hpss_userattr_t * a, size_t cnt)
{
    if (a == NULL)
        return PTR(a);

    char * str = "[";
    for (int i = 0; i < cnt; i++)
    {
        str = _sprintf(
            pool,
            "%s%s%s",
            i == 0 ? "" : str, // prefix
            i == 0 ? "" : ", ",
            HPSS_USERATTR_T(a[i]));
    }
    return _strcat(pool, str, "]");
}

char *
_hpss_userattr_list_t_ptr(struct pool * pool, const hpss_userattr_list_t * p)
{
    if (p == NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "len=%s, " // int
            "Pair=%s"  // hpss_userattr_t *
        "}",
            INT(p->len),
            _hpss_userattr_list_t_array(pool, p->Pair, p->len));
}

char *
_hpssoid_t(struct pool * pool, hpssoid_t o)
{
#if HPSS_MAJOR_VERSION >= 8
    return _sprintf(
        pool,
        "{"
            "bytes={"
                "%s, " // 1
                "%s, " // 2
                "%s, " // 3
                "%s, " // 4
                "%s, " // 5
                "%s, " // 6
                "%s, " // 7
                "%s, " // 8
                "%s, " // 9
                "%s, " // 10
                "%s, " // 11
                "%s, " // 12
                "%s, " // 13
                "%s, " // 14
                "%s, " // 15
                "%s, " // 16
                "%s, " // 17
                "%s, " // 18
                "%s"   // 19 (KSOID)
            "}"
        "}",
            HEX8(o.Bytes[ 0]),
            HEX8(o.Bytes[ 1]),
            HEX8(o.Bytes[ 2]),
            HEX8(o.Bytes[ 3]),
            HEX8(o.Bytes[ 4]),
            HEX8(o.Bytes[ 5]),
            HEX8(o.Bytes[ 6]),
            HEX8(o.Bytes[ 7]),
            HEX8(o.Bytes[ 8]),
            HEX8(o.Bytes[ 9]),
            HEX8(o.Bytes[10]),
            HEX8(o.Bytes[11]),
            HEX8(o.Bytes[12]),
            HEX8(o.Bytes[13]),
            HEX8(o.Bytes[14]),
            HEX8(o.Bytes[15]),
            HEX8(o.Bytes[16]),
            HEX8(o.Bytes[17]),
            HEX8(o.Bytes[18]));
#else
    return _sprintf(
        pool,
        "{"
            "ObjectID=%s, "            // hpss_uuid_t
            "ServerDep1=%s, "          // unsigned32
            "ServerDep2=%s, "          // unsigned16
            "ServerDep3=%s, "          // unsigned16
            "ServerDep4=%s, "          // byte
            "ServerDep5=%s, "          // byte
            "SecurityLevel={%s, %s}, " // byte[2]
            "Reserved={%s, %s}, "      // byte[2]
            "SubType=%s, "             // byte
            "Type=%s"                  // byte
        "}",
            HPSS_UUID_T(o.ObjectID),
            UNSIGNED(o.ServerDep1),
            UNSIGNED16(o.ServerDep2),
            UNSIGNED16(o.ServerDep3),
            HEX8(o.ServerDep4),
            HEX8(o.ServerDep5),
            HEX8(o.SecurityLevel[0]), HEX8(o.SecurityLevel[1]),
            HEX8(o.Reserved[0]), HEX8(o.Reserved[1]),
            HEX8(o.SubType),
            HEX8(o.Type));
#endif
}

char *
_ns_direntry_t(struct pool * pool, ns_DirEntry_t e)
{
    return _sprintf(
        pool,
        "{"
            "Name=%s, "      // char[HPSS_MAX_FILE_NAME]
            "ObjHandle=%s, " // ns_ObjHandle_t
            "ObjOffset=%s, " // u_signed64
            "Attrs=%s",      // hpss_Attrs_t
        "}",
            CHAR_PTR(e.Name),
            NS_OBJHANDLE_T(e.ObjHandle),
            UNSIGNED64(e.ObjOffset),
            HPSS_ATTRS_T(e.Attrs));
}

// TODO: duplicate code of _pv_list_val
char *
_ns_direntry_t_array(struct pool * pool, const ns_DirEntry_t * a, size_t cnt)
{
    if (a == NULL)
        return PTR(a);

    char * str = "[";
    for (int i = 0; i < cnt; i++)
    {
        str = _sprintf(
            pool,
            "%s%s%s",
            i == 0 ? "" : str, // prefix
            i == 0 ? "" : ", ",
            NS_DIRENTRY_T(a[i]));
    }
    return _strcat(pool, str, "]");
}

char *
_ns_filesetattrs_t_ptr(struct pool * pool, const ns_FilesetAttrs_t * p)
{
    return _sprintf(
        pool,
        "{"
            "RegisterBitMap=%s, "        // u_signed64
            "ChangedRegisterBitMap=%s, " // u_signed64
            "ClassOfService=%s, "        // unsigned32
            "FamilyId=%s, "              // unsigned32
            "FilesetHandle=%s, "         // ns_ObjHandle_t
            "FilesetId=%s, "             // u_signed64
            "FilesetName=%s, "           // char[HPSS_MAX_FS_NAME_LENGTH]
            "FilesetType=%s, "           // unsigned32
#if HPSS_MAJOR_VERSION == 7
            "GatewayUUID=%s, "           // hpss_uuid_t
#endif
            "StateFlags=%s, "            // unsigned32
            "SubSystemId=%s, "           // unsigned32
// XXX            "UserData=%s, "              // byte[NS_FS_MAX_USER_DATA]
            "DirectoryCount=%s, "        // u_signed64
            "FileCount=%s, "             // u_signed64
            "HardLinkCount=%s, "         // u_signed64
            "JunctionCount=%s, "         // u_signed64
            "SymLinkCount=%s"            // u_signed64
        "}",
            HEX64(p->RegisterBitMap),
            HEX64(p->ChangedRegisterBitMap),
            UNSIGNED(p->ClassOfService),
            UNSIGNED(p->FamilyId),
            NS_OBJHANDLE_T(p->FilesetHandle),
            UNSIGNED64(p->FilesetId),
            CHAR_PTR(p->FilesetName),
            UNSIGNED(p->FilesetType),
#if HPSS_MAJOR_VERSION == 7
            HPSS_UUID_T(p->GatewayUUID),
#endif
            UNSIGNED(p->StateFlags),
            UNSIGNED(p->SubSystemId),
// XXX            CHAR_PTR(p->UserData),
            UNSIGNED64(p->DirectoryCount),
            UNSIGNED64(p->FileCount),
            UNSIGNED64(p->HardLinkCount),
            UNSIGNED64(p->JunctionCount),
            UNSIGNED64(p->SymLinkCount));
}

char *
_ns_objhandle_t(struct pool * pool, ns_ObjHandle_t o)
{
    return _ns_objhandle_t_ptr(pool, &o);
}

char *
_ns_objhandle_t_ptr(struct pool * pool, const ns_ObjHandle_t * p)
{
    if (p == NULL)
        return PTR(p);

#if HPSS_MAJOR_VERSION >= 8
    return _sprintf(
        pool,
        "{"
            "ObjId=%s, "      // uint64_t
            "ObjNsHash=%s, "  // hpss_distributionkey_t
            "FileId=%s, "     // uint64_t
            "FileNsHash=%s, " // hpss_distributionkey_t
            "Type=%s, "       // byte
            "Flags=%s, "      // byte
            "Generation=%s, " // uint64_t
            "CoreServerId=%s" // hpss_srvr_id_t
        "}",
            UNSIGNED64(p->ObjId),
            HPSS_DISTRIBUTIONKEY_T(p->ObjNsHash),
            UNSIGNED64(p->FileId),
            HPSS_DISTRIBUTIONKEY_T(p->FileNsHash),
            HEX8(p->Type),
            HEX8(p->Flags),
            UNSIGNED64(p->Generation),
            HPSS_SRVR_ID_T(p->CoreServerId));
#else
    return _sprintf(
        pool,
        "{"
            "ObjId=%s, "        // u_signed64
            "FileId=%s, "       // u_signed64
            "Type=%s, "         // byte
            "Flags=%s, "        // byte
            "Generation=%s, "   // unsigned16
            "CoreServerUUID=%s" // hpss_uuid_t
        "}",
            UNSIGNED64(p->ObjId),
            UNSIGNED64(p->FileId),
            HEX8(p->Type),
            HEX8(p->Flags),
            UNSIGNED16(p->Generation),
            HPSS_UUID_T(p->CoreServerUUID));
#endif
}

char *
_pv_list_element_t(struct pool * pool, pv_list_element_t e)
{
    return _sprintf(
        pool,
        "{"
            "Name=%s, " // char[HPSS_PV_NAME_SIZE]
            "Flags=%s"  // unsigned32
        "}",
        CHAR_PTR(e.Name),
        HEX(e.Flags));
}

// TODO: duplicate code of _unsigned_array
static char *
_pv_list_val(struct pool * pool, unsigned len, pv_list_element_t * list)
{
    char * str = "[";
    for (int i = 0; i < len; i++)
    {
        str = _sprintf(
            pool,
            "%s%s%s",
            i == 0 ? "" : str, // prefix
            i == 0 ? "" : ", ",
            PV_LIST_ELEMENT_T(list[i]));
    }
    return _strcat(pool, str, "]");
}

char *
_pv_list_t_ptr(struct pool * pool, const pv_list_t * l)
{
    if (l == NULL)
        return PTR(l);

    return _sprintf(
        pool,
        "{"
            "List={"
                "List_len=%s, " // u_int
                "List_val=%s"   // pv_list_element_t[]
            "}"
        "}",
            UNSIGNED(l->List.List_len),
            _pv_list_val(pool, l->List.List_len, l->List.List_val));
}

char *
_sec_cred_t_ptr(struct pool * pool, const sec_cred_t * p)
{
    if (p == NULL)
        return PTR(p);

    return _sprintf(
        pool,
        "{"
            "Name=%s, "       // char[HPSS_MAX_USER_NAME]
            "RealmName=%s, "  // char[HPSS_MAX_REALM_NAME]
            "Directory=%s, "  // char[HPSS_MAX_PATH_NAME]
            "UserShell=%s, "  // char[HPSS_MAX_USER_SHELL]
            "RealmId=%s, "    // unsigned32
            "Uid=%s, "        // unsigned32
            "Gid=%s, "        // unsigned32
            "Uuid=%s, "       // hpss_uuid_t
            "DefAccount=%s, " // acct_rec_t
            "CurAccount=%s, " // acct_rec_t
            "NumGroups=%s, "  // unsigned32
            "AltGroups=%s, "  // unsigned32[HPSS_NGROUPS_MAX]
        "}",
            CHAR_PTR(p->Name),
            CHAR_PTR(p->RealmName),
            CHAR_PTR(p->Directory),
            CHAR_PTR(p->UserShell),
            UNSIGNED(p->RealmId),
            UNSIGNED(p->Uid),
            UNSIGNED(p->Gid),
            HPSS_UUID_T(p->Uuid),
            ACCT_REC_T(p->DefAccount),
            ACCT_REC_T(p->CurAccount),
            UNSIGNED(p->NumGroups),
            UNSIGNED_ARRAY(p->AltGroups, p->NumGroups));
}

char *
_timestamp_sec_t_ptr(struct pool * pool, const timestamp_sec_t * p)
{
    if (p == NULL)
        return PTR(p);
    return TIMESTAMP_SEC_T(*p);
}


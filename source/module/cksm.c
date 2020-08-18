/*
 * System includes
 */
#include <assert.h>

/*
 * Local includes
 */
#include "cksm.h"
#include "hpss.h"
#include "pio.h"
#include "stat.h"
#include "logging.h"

int
cksm_pio_callout(char *    Buffer,
                 uint32_t *Length,
                 uint64_t  Offset,
                 void *    CallbackArg);

void
cksm_transfer_complete_callback(globus_result_t Result, void *UserArg);

void
cksm_update_markers(cksm_marker_t *Marker, globus_off_t Bytes)
{
    if (Marker)
    {
        pthread_mutex_lock(&Marker->Lock);
        {
            Marker->TotalBytes += Bytes;
        }
        pthread_mutex_unlock(&Marker->Lock);
    }
}

void
cksm_send_markers(void *UserArg)
{
    cksm_marker_t *marker = (cksm_marker_t *)UserArg;
    char           total_bytes_string[128];

    pthread_mutex_lock(&marker->Lock);
    {
        /* Convert the byte count to a string. */
        sprintf(
            total_bytes_string, "%" GLOBUS_OFF_T_FORMAT, marker->TotalBytes);

        /* Send the intermediate response. */
        globus_gridftp_server_intermediate_command(
            marker->Operation, GLOBUS_SUCCESS, total_bytes_string);
    }
    pthread_mutex_unlock(&marker->Lock);
}

globus_result_t
cksm_start_markers(cksm_marker_t **Marker, globus_gfs_operation_t Operation)
{
    int              marker_freq = 0;
    globus_reltime_t delay;
    globus_result_t  result = GLOBUS_SUCCESS;

    /* Get the frequency for maker updates. */
    globus_gridftp_server_get_update_interval(Operation, &marker_freq);

    if (marker_freq > 0)
    {
        *Marker = malloc(sizeof(cksm_marker_t));
        if (!*Marker)
            return GlobusGFSErrorMemory("cksm_marker_t");

        pthread_mutex_init(&(*Marker)->Lock, NULL);
        (*Marker)->TotalBytes = 0;
        (*Marker)->Operation  = Operation;

        /* Setup the periodic callback. */
        GlobusTimeReltimeSet(delay, marker_freq, 0);
        result = globus_callback_register_periodic(&(*Marker)->CallbackHandle,
                                                   &delay,
                                                   &delay,
                                                   cksm_send_markers,
                                                   (*Marker));
        if (result)
        {
            free(*Marker);
            *Marker = NULL;
        }
    }

    return result;
}

void
cksm_stop_markers(cksm_marker_t *Marker)
{
    if (Marker)
    {
        globus_callback_unregister(Marker->CallbackHandle, NULL, NULL, NULL);
        pthread_mutex_destroy(&Marker->Lock);
        free(Marker);
    }
}

globus_result_t
cksm_open_for_reading(char *Pathname, int *FileFD, int *FileStripeWidth)
{
    hpss_cos_hints_t      hints_in;
    hpss_cos_hints_t      hints_out;
    hpss_cos_priorities_t priorities;

    *FileFD = -1;

    /* Initialize the hints in. */
    memset(&hints_in, 0, sizeof(hpss_cos_hints_t));

    /* Initialize the hints out. */
    memset(&hints_out, 0, sizeof(hpss_cos_hints_t));

    /* Initialize the priorities. */
    memset(&priorities, 0, sizeof(hpss_cos_priorities_t));

    /* Open the HPSS file. */
    *FileFD = Hpss_Open(Pathname,
                        O_RDONLY,
                        S_IRUSR | S_IWUSR,
                        &hints_in,
                        &priorities,
                        &hints_out);
    if (*FileFD < 0)
        return GlobusGFSErrorSystemError("hpss_Open", -(*FileFD));

    /* Copy out the file stripe width. */
    *FileStripeWidth = hints_out.StripeWidth;

    return GLOBUS_SUCCESS;
}

int
cksm_pio_callout(char *    Buffer,
                 uint32_t *Length,
                 uint64_t  Offset,
                 void *    CallbackArg)
{
    int          rc        = 0;
    cksm_info_t *cksm_info = CallbackArg;

    assert(*Length <= cksm_info->BlockSize);

    rc = MD5_Update(&cksm_info->MD5Context, Buffer, *Length);
    if (rc != 1)
    {
        cksm_info->Result = GlobusGFSErrorGeneric("MD5_Update() failed");
        return 1;
    }

    cksm_update_markers(cksm_info->Marker, *Length);

    return 0;
}

void
cksm_range_complete_callback(globus_off_t *Offset,
                             globus_off_t *Length,
                             int *         Eot,
                             void *        UserArg)
{
    cksm_info_t *cksm_info = UserArg;

    *Offset += *Length;
    cksm_info->RangeLength -= *Length;
    *Length = cksm_info->RangeLength;

    if (*Length == 0)
        *Eot = 1;
}

void
cksm_transfer_complete_callback(globus_result_t Result, void *UserArg)
{
    globus_result_t result    = Result;
    cksm_info_t *   cksm_info = UserArg;
    int             rc        = 0;
    unsigned char   md5_digest[MD5_DIGEST_LENGTH];
    char            cksm_string[2 * MD5_DIGEST_LENGTH + 1];
    int             i;

    /* Give our error priority. */
    if (cksm_info->Result)
        result = cksm_info->Result;

    rc = Hpss_Close(cksm_info->FileFD);
    if (rc && !result)
        result = GlobusGFSErrorSystemError("hpss_Close", -rc);

    if (!result)
    {
        rc = MD5_Final(md5_digest, &cksm_info->MD5Context);
        if (rc != 1)
            result = GlobusGFSErrorGeneric("MD5_Final() failed");
    }

    if (!result)
    {
        for (i = 0; i < MD5_DIGEST_LENGTH; i++)
        {
            sprintf(&(cksm_string[i * 2]), "%02x", (unsigned int)md5_digest[i]);
        }
    }

    cksm_stop_markers(cksm_info->Marker);

    cksm_info->Callback(
        cksm_info->Operation, result, result ? NULL : cksm_string);

    if (!result && cksm_info->CommandInfo->cksm_offset == 0 &&
        cksm_info->CommandInfo->cksm_length == -1)
    {
        if (cksm_info->UseUDAChecksums)
            cksm_set_uda_checksum(cksm_info->Pathname, cksm_string);
    }

    free(cksm_info->Pathname);
    free(cksm_info);
}

void
cksm(globus_gfs_operation_t     Operation,
     globus_gfs_command_info_t *CommandInfo,
     bool                       UseUDAChecksums,
     commands_callback          Callback)
{
    globus_result_t result            = GLOBUS_SUCCESS;
    cksm_info_t *   cksm_info         = NULL;
    int             rc                = 0;
    int             file_stripe_width = 0;
    char *          checksum_string   = NULL;
    hpss_stat_t     hpss_stat_buf;

    if (CommandInfo->cksm_offset == 0 && CommandInfo->cksm_length == -1)
    {
        if (UseUDAChecksums)
        {
            result = cksm_get_uda_checksum(CommandInfo->pathname,
                                           &checksum_string);
            if (result || checksum_string)
            {
                Callback(Operation, result, result ? NULL : checksum_string);
                if (checksum_string)
                    free(checksum_string);
                return;
            }
        }
    }

    rc = Hpss_Stat(CommandInfo->pathname, &hpss_stat_buf);
    if (rc)
    {
        result = GlobusGFSErrorSystemError("hpss_Stat", -rc);
        Callback(Operation, result, NULL);
        return;
    }

    cksm_info = malloc(sizeof(cksm_info_t));
    if (!cksm_info)
    {
        result = GlobusGFSErrorMemory("cksm_info_t");
        goto cleanup;
    }
    memset(cksm_info, 0, sizeof(cksm_info_t));
    cksm_info->Operation       = Operation;
    cksm_info->CommandInfo     = CommandInfo;
    cksm_info->Callback        = Callback;
    cksm_info->UseUDAChecksums = UseUDAChecksums;
    cksm_info->FileFD          = -1;
    cksm_info->Pathname        = strdup(CommandInfo->pathname);
    cksm_info->RangeLength     = CommandInfo->cksm_length;
    if (cksm_info->RangeLength == -1)
        cksm_info->RangeLength =
            hpss_stat_buf.st_size - CommandInfo->cksm_offset;

    rc = MD5_Init(&cksm_info->MD5Context);
    if (rc != 1)
    {
        result = GlobusGFSErrorGeneric("Failed to create MD5 context");
        goto cleanup;
    }

    globus_gridftp_server_get_block_size(Operation, &cksm_info->BlockSize);

    /*
     * Open the file.
     */
    result = cksm_open_for_reading(
        CommandInfo->pathname, &cksm_info->FileFD, &file_stripe_width);
    if (result)
        goto cleanup;

    result = cksm_start_markers(&cksm_info->Marker, Operation);
    if (result)
        goto cleanup;

    /*
     * Setup PIO
     */
    result = pio_start(HPSS_PIO_READ,
                       cksm_info->FileFD,
                       file_stripe_width,
                       cksm_info->BlockSize,
                       CommandInfo->cksm_offset,
                       cksm_info->RangeLength,
                       cksm_pio_callout,
                       cksm_range_complete_callback,
                       cksm_transfer_complete_callback,
                       cksm_info);

cleanup:
    if (result)
    {
        if (cksm_info)
        {
            if (cksm_info->FileFD != -1)
                Hpss_Close(cksm_info->FileFD);
            if (cksm_info->Pathname)
                free(cksm_info->Pathname);
            free(cksm_info);
        }
        Callback(Operation, result, NULL);
    }
}

/*
 * /hpss/user/cksum/algorithm                                  md5
 * /hpss/user/cksum/checksum               93b885adfe0da089cdf634904fd59f71
 * /hpss/user/cksum/lastupdate                          1376424299
 * /hpss/user/cksum/errors                                       0
 * /hpss/user/cksum/state                                    Valid
 * /hpss/user/cksum/app                                    hpsssum
 * /hpss/user/cksum/filesize                                     1
 */
globus_result_t
cksm_set_uda_checksum(char *Pathname, char *Checksum)
{
    int                  retval = 0;
    char                 filesize_buf[32];
    char                 lastupdate_buf[32];
    globus_result_t      result = GLOBUS_SUCCESS;
    hpss_userattr_t      user_attrs[7];
    hpss_userattr_list_t attr_list;
    globus_gfs_stat_t    gfs_stat;

    result = stat_object(Pathname, &gfs_stat);
    if (result != GLOBUS_SUCCESS)
        return result;

    snprintf(filesize_buf, sizeof(filesize_buf), "%lu", gfs_stat.size);
    snprintf(lastupdate_buf, sizeof(lastupdate_buf), "%lu", time(NULL));
    stat_destroy(&gfs_stat);

    attr_list.len  = sizeof(user_attrs) / sizeof(*user_attrs);
    attr_list.Pair = user_attrs;

    attr_list.Pair[0].Key   = "/hpss/user/cksum/algorithm";
    attr_list.Pair[0].Value = "md5";
    attr_list.Pair[1].Key   = "/hpss/user/cksum/checksum";
    attr_list.Pair[1].Value = Checksum;
    attr_list.Pair[2].Key   = "/hpss/user/cksum/lastupdate";
    attr_list.Pair[2].Value = lastupdate_buf;
    attr_list.Pair[3].Key   = "/hpss/user/cksum/errors";
    attr_list.Pair[3].Value = "0";
    attr_list.Pair[4].Key   = "/hpss/user/cksum/state";
    attr_list.Pair[4].Value = "Valid";
    attr_list.Pair[5].Key   = "/hpss/user/cksum/app";
    attr_list.Pair[5].Value = "GridFTP";
    attr_list.Pair[6].Key   = "/hpss/user/cksum/filesize";
    attr_list.Pair[6].Value = filesize_buf;

    retval = Hpss_UserAttrSetAttrs(Pathname, &attr_list, NULL);
    if (retval)
        return GlobusGFSErrorSystemError("hpss_UserAttrSetAttrs", -retval);

    return GLOBUS_SUCCESS;
}

globus_result_t
cksm_get_uda_checksum(char *  Pathname, char ** ChecksumString)
{
    int                  retval = 0;
    char *               tmp    = NULL;
    char                 value[HPSS_XML_SIZE];
    char                 state[HPSS_XML_SIZE];
    char                 algorithm[HPSS_XML_SIZE];
    char                 checksum[HPSS_XML_SIZE];
    hpss_userattr_t      user_attrs[3];
    hpss_userattr_list_t attr_list;

    *ChecksumString = NULL;

    attr_list.len  = sizeof(user_attrs) / sizeof(*user_attrs);
    attr_list.Pair = user_attrs;

    attr_list.Pair[0].Key   = "/hpss/user/cksum/algorithm";
    attr_list.Pair[0].Value = algorithm;
    attr_list.Pair[1].Key   = "/hpss/user/cksum/checksum";
    attr_list.Pair[1].Value = checksum;
    attr_list.Pair[2].Key   = "/hpss/user/cksum/state";
    attr_list.Pair[2].Value = state;


#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION < 4)
    retval = Hpss_UserAttrGetAttrs(Pathname, &attr_list, UDA_API_VALUE);
#else
    retval = Hpss_UserAttrGetAttrs(Pathname,
                                   &attr_list,
                                   UDA_API_VALUE,
                                   HPSS_XML_SIZE - 1);
#endif

    switch (retval)
    {
    case 0:
        break;
    case -ENOENT:
        return GLOBUS_SUCCESS;
    default:
        return GlobusGFSErrorSystemError("hpss_UserAttrGetAttrs", -retval);
    }

    tmp = Hpss_ChompXMLHeader(algorithm, NULL);
    if (!tmp)
        return GLOBUS_SUCCESS;

    strcpy(value, tmp);
    free(tmp);

    if (strcmp(value, "md5") != 0)
        return GLOBUS_SUCCESS;

    tmp = Hpss_ChompXMLHeader(state, NULL);
    if (!tmp)
        return GLOBUS_SUCCESS;

    strcpy(value, tmp);
    free(tmp);

    if (strcmp(value, "Valid") != 0)
        return GLOBUS_SUCCESS;

    *ChecksumString = Hpss_ChompXMLHeader(checksum, NULL);
    return GLOBUS_SUCCESS;
}

globus_result_t
cksm_clear_uda_checksum(char *Pathname)
{
    int                  retval = 0;
    hpss_userattr_t      user_attrs[1];
    hpss_userattr_list_t attr_list;

    attr_list.len  = sizeof(user_attrs) / sizeof(*user_attrs);
    attr_list.Pair = user_attrs;

    attr_list.Pair[0].Key   = "/hpss/user/cksum/state";
    attr_list.Pair[0].Value = "Invalid";

    retval = Hpss_UserAttrSetAttrs(Pathname, &attr_list, NULL);
    if (retval && retval != -ENOENT)
        return GlobusGFSErrorSystemError("hpss_UserAttrSetAttrs", -retval);

    return GLOBUS_SUCCESS;
}

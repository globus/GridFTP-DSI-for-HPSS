#ifndef HPSS_DSI_UTILS_H
#define HPSS_DSI_UTILS_H

/*
 * System includes
 */
#include <stdbool.h>

/*
 * Local includes
 */
#include "hpss.h"

/*
 * In HPSS 7.5, the bitfile ID changed from an hpssoid_t stored at x.Attrs.BitfileId
 * to a bfs_bitfile_obj_handle_t stored at Attrs.BitfileObj.BfId. These macros help
 * to simplify older code branches that are still in use and also reminds us of where
 * the bitfile ID lives.
 */

#if (HPSS_MAJOR_VERSION == 7 && HPSS_MINOR_VERSION > 4) || HPSS_MAJOR_VERSION >= 8
  #define bitfile_id_t bfs_bitfile_obj_handle_t
  #define ATTR_TO_BFID(x) (x.Attrs.BitfileObj.BfId)
#else
  #define bitfile_id_t hpssoid_t
  #define ATTR_TO_BFID(x) (x.Attrs.BitfileId)
#endif

/*
 * Returns True if UUIDString is a non-null value with the format:
 *    "[hex]{8}-[hex]{4}-[hex]{4}-[hex]{4}-[hex]{12}\0"
 * The string can use upper or lower case characters.
 */
bool
is_valid_uuid(const char * UUIDString);

/*
 * Generate CallbackID from TaskID and BitfileID.
 *   TaskID - Required
 *   BitfileID - Optional
 *
 * If BitfileID is NULL, CallbackID is set to TaskID. If BitfileID is not NULL,
 * CallbackID is TaskID^BitfileID (TaskID is first converted byte-by-byte to its
 * int values).
 */
globus_result_t
generate_callback_id(
    const char                  *  TaskID,
    bitfile_id_t                *  BitfileID,
    hpss_reqid_t                *  CallbackID);

#if HPSS_MAJOR_VERSION >= 8
/*
 * Converts a hpss_reqid_t * (aka a hpss_uuid_t) to a UUID in string format:
 *   ex. hpss_request_id * => "ddfeb23c-53ee-435b-8318-a2c4fb2519d2"
 *
 * Added with batch staging in 9.3.
 */
globus_result_t
hpss_reqid_to_string(
    const hpss_reqid_t          *  RequestID,
    char                        ** UUIDString);

/*
 * Converts a UUID string to hpss_reqid_t *.
 *   ex. "ddfeb23c-53ee-435b-8318-a2c4fb2519d2" => hpss_request_id
 *
 * Added with batch staging in 9.3.
 */
globus_result_t
string_to_hpss_reqid(
    const char                  *  UUIDString,
    hpss_reqid_t                *  RequestID);
#endif // HPSS_MAJOR_VERSION >= 8

#endif /* HPSS_DSI_UTILS_H */

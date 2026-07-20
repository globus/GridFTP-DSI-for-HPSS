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

bool
is_valid_uuid(const char * uuid_str);

#define UUID_BYTE_COUNT 16

// Returns an array of UUID_BYTE_COUNT bytes. Not NULL-terminated.
void
uuid_str_to_bytes(const char * UUID, unsigned char Bytes[UUID_BYTE_COUNT]);

// Returns an array of UUID_BYTE_COUNT bytes. Not NULL-terminated.
void
hpss_uuid_to_bytes(const hpss_uuid_t * UUID, unsigned char Bytes[UUID_BYTE_COUNT]);

void
bytes_to_hpss_uuid(const unsigned char Bytes[UUID_BYTE_COUNT], hpss_uuid_t * UUID);

void
bytes_to_unsigned(const unsigned char Bytes[UUID_BYTE_COUNT], unsigned * Unsigned);

#define UUID_STR_COUNT 37 // 36 characters + 1 null terminator

/*
 * Translate Bytes to a UUID in string format, ex. "ddfeb23c-53ee-435b-8318-a2c4fb2519d2"
 */
void
uuid_bytes_to_str(const unsigned char Bytes[UUID_BYTE_COUNT], char UUID[UUID_STR_COUNT]);

#endif /* HPSS_DSI_UTILS_H */

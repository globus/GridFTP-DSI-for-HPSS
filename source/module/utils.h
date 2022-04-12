#ifndef HPSS_DSI_UTILS_H
#define HPSS_DSI_UTILS_H

#include <stdbool.h>
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

#endif /* HPSS_DSI_UTILS_H */

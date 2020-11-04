#ifndef _EXPECTATIONS_H_
#define _EXPECTATIONS_H_

#include <stdbool.h>
#include <stdarg.h>
#include <stdint.h>

typedef enum {
    WHEN_ONCE,
    WHEN_ALWAYS,
} when_t;

typedef enum {
    UNSUPPORTED,
    INT,
    UINT64,
    MEMORY,
} value_type_t;

struct value {
    value_type_t Type;
    union {
        int INT;
        uint64_t UINT64;
        void * MEMORY;
    } _u;
};

void
set_expected_params(const char * Func,
                    when_t       When,
                    const char * Param,
                    value_type_t Type,
                    ...); // Value [, "Param", Type, Value]*, NULL

void
check_params(const char * Function,
             const char * Parameter,
             value_type_t Type,
             ...); // Value [, "Param", Type, Value[, Size]], NULL

void
set_expected_return(const char * Function,
                    when_t       When,
                    value_type_t Type,
                    ...); // Value [, "Param", Type, Value]

struct value
get_return(const char * Function,
           value_type_t Type,
           ...); // DefaultValue [, "Param", Type, Value[, Size]], NULL

void
reset_expectations();

#endif /* _EXPECTATIONS_H_ */

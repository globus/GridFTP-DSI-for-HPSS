#ifndef _MOCKING_H_
#define _MOCKING_H_

#include <stdlib.h>
#include "_expectations.h"

// Usage ("Func", When, "Param", Type, Value [, "Param", Type, Value]*)
#define EXPECT_PARAMS(Func, When, Param, Type, ...) \
    set_expected_params(Func, When, Param, Type, __VA_ARGS__, NULL)

// Usage ("Param", Type, Value [, "Param", Type, Value[, size]]*)
#define CHECK_PARAMS(Param, Type, ...) \
    check_params(__func__, Param, Type, __VA_ARGS__, NULL)

// Usage ("Func", When, Type, Value [, "Param", Type, Value]*)
#define EXPECT_RETURN(Func, When, Type, ...) \
    set_expected_return(Func, When, Type, __VA_ARGS__, NULL)

// Usage (Type, DefaultValue [, "Param", Type, Value[, Size]]*)
#define GET_RETURN(Type, ...) \
    get_return(__func__, Type, __VA_ARGS__, NULL)._u.Type

#endif /* _MOCKING_H_ */

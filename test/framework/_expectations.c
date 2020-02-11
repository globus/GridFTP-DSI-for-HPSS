#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <assert.h>
#include "_expectations.h"

struct expected_params {
    const char * Func;
    when_t       When;

    struct param {
        const char * Param;
        struct value Value;
    } ** Params;

    struct expected_params * Next;
};

static struct expected_params * ExpectedParams = NULL;

static struct value
get_value_by_type(value_type_t Type, va_list ap)
{
    struct value value = {.Type = Type};

    switch (Type)
    {
    case INT:
        value._u.INT = va_arg(ap, int);
        break;
    case UINT64:
        value._u.UINT64 = va_arg(ap, uint64_t);
        break;
    case MEMORY:
        value._u.MEMORY = va_arg(ap, void *);
        break;
    case UNSUPPORTED: // Add new types to _expectations.h
        assert(0);
    }
    return value;
}

void
set_expected_params(const char * Func,
                    when_t       When,
                    const char * Param,
                    value_type_t Type,
                    ...)  // Value [, "Param", Type, Value]*, NULL
{
    struct expected_params * exp_params = calloc(1, sizeof(*exp_params));

    exp_params->Func = Func;
    exp_params->When = When;

    va_list ap;
    va_start(ap, Type);
    {
        int count = 0;
        const char * param = Param;
        value_type_t type  = Type;

        do {
            // Add this param to Params
            size_t size = (count+2)*sizeof(*exp_params->Params);
            exp_params->Params = realloc(exp_params->Params, size);

            exp_params->Params[count] = calloc(1, sizeof(**exp_params->Params));
            exp_params->Params[count]->Param = param;
            exp_params->Params[count]->Value = get_value_by_type(type, ap);

            // Terminate the array
            exp_params->Params[count+1] = NULL;

            // Fetch next 
            param = va_arg(ap, const char *);
            if (param)
                type = va_arg(ap, value_type_t);

        } while (param && count++);
    }
    va_end(ap);

    struct expected_params ** global_list_ptr = &ExpectedParams;
    while (*global_list_ptr != NULL)
        global_list_ptr = &(*global_list_ptr)->Next;
    *global_list_ptr = exp_params;
}

struct expected_params *
find_expected_params(struct expected_params * ExpParams, const char * Func)
{
    struct expected_params * exp_params = NULL;
    for (exp_params = ExpParams; exp_params; exp_params = exp_params->Next)
    {
        if (strcmp(exp_params->Func, Func) == 0)
            return exp_params;
    }
    return NULL;
}

struct param *
find_param(struct param ** Params, const char * Param)
{
    if (!Params)
        return NULL;

    for (int i = 0; Params[i]; i++)
    {
        if (strcmp(Params[i]->Param, Param) == 0)
            return Params[i];
    }
    return NULL;
}

void
remove_expected_params(struct expected_params ** ExpParamsList,
                       struct expected_params  * ExpParamsEntry)
{
    struct expected_params ** listptr = ExpParamsList;

    while (*listptr && *listptr != ExpParamsEntry)
    {
        listptr = &(*listptr)->Next;
    }

    if (*listptr)
        *listptr = ExpParamsEntry->Next;
}

bool
compare_values(struct value V1, struct value V2, size_t Size)
{
    if (V1.Type != V2.Type)
        return false;

    switch (V1.Type)
    {
    case INT:
        return (V1._u.INT == V2._u.INT);
    case UINT64:
        return (V1._u.UINT64 == V2._u.UINT64);
    case MEMORY:
        return (memcmp(V1._u.MEMORY, V2._u.MEMORY, Size) == 0);
    default:
        assert(0); // Hard fault on programming error
    }
    return false;
}

void
check_params(const char * Func,
             const char * Param,
             value_type_t Type,
             ...) // Value [, "Param", Type, Value[, Size]], NULL
{
    struct expected_params * exp_params = NULL;

    exp_params = find_expected_params(ExpectedParams, Func);
    if (!exp_params)
        return;

    va_list ap;
    va_start(ap, Type);
    {
        const char * param = Param;
        value_type_t type  = Type;

        do {
            struct value value = get_value_by_type(type, ap);
            size_t size = 0;
            if (type == MEMORY)
                size = va_arg(ap, size_t);

            struct param * exp_param = find_param(exp_params->Params, param);
            if (exp_param)
            {
                // XXX Improve this
                assert(compare_values(value, exp_param->Value, size));
            }

            // Fetch next 
            param = va_arg(ap, const char *);
            if (param)
                type = va_arg(ap, value_type_t);

        } while (param);
    }
    va_end(ap);

    if (exp_params->When == WHEN_ONCE)
        remove_expected_params(&ExpectedParams, exp_params);
}

struct expected_returns {
    const char    * Func;
    when_t          When;
    struct value    Value;
    struct param ** Params;

    struct expected_returns * Next;
};

static struct expected_returns * ExpectedReturns = NULL;

void
set_expected_return(const char * Func,
                    when_t       When,
                    value_type_t Type,
                    ...) // Value [, "Param", Type, Value]
{
    struct expected_returns * returns = calloc(1, sizeof(*returns));

    returns->Func = Func;
    returns->When = When;

    va_list ap;
    va_start(ap, Type);
    {
        returns->Value = get_value_by_type(Type, ap);

        int count = 0;
        const char * param;
        while ((param = va_arg(ap, const char *)))
        {
            value_type_t type = va_arg(ap, value_type_t);
            struct value value = get_value_by_type(type, ap);

            // Add this param to Params
            size_t size = (count+2)*sizeof(*returns->Params);
            returns->Params = realloc(returns->Params, size);

            returns->Params[count] = calloc(1, sizeof(**returns->Params));
            returns->Params[count]->Param = param;
            returns->Params[count]->Value = value;

            // Terminate the array
            returns->Params[count+1] = NULL;
        }
    }
    va_end(ap);

    struct expected_returns ** global_list_ptr = &ExpectedReturns;
    while (*global_list_ptr != NULL)
        global_list_ptr = &(*global_list_ptr)->Next;
    *global_list_ptr = returns;
}

struct expected_returns *
find_expected_returns(struct expected_returns * ExpReturns, const char * Func)
{
    struct expected_returns * returns = NULL;
    for (returns = ExpReturns; returns; returns = returns->Next)
    {
        if (strcmp(returns->Func, Func) == 0)
            return returns;
    }
    return NULL;
}

void
remove_expected_returns(struct expected_returns ** ExpReturnsList,
                        struct expected_returns  * ExpReturnsEntry)
{
    struct expected_returns ** listptr = ExpReturnsList;

    while (*listptr && *listptr != ExpReturnsEntry)
    {
        listptr = &(*listptr)->Next;
    }

    if (*listptr)
        *listptr = ExpReturnsEntry->Next;
}

void
set_return_value(struct value SourceValue, void * Destination, size_t Size)
{
    switch (SourceValue.Type)
    {
    case INT:
        assert(Size == sizeof(int));
        *((int *)Destination) = SourceValue._u.INT;
        break;
    case UINT64:
        assert(Size == sizeof(uint64_t));
        *((uint64_t *)Destination) = SourceValue._u.UINT64;
        break;
    case MEMORY:
        memcpy(Destination, SourceValue._u.MEMORY, Size);
        break;
    default:
        assert(0); // Hard fault on programming error
    }
}

struct value
get_return(const char * Func,
           value_type_t Type,
           ...) // DefaultValue [, "Param", Type, Value[, Size]], NULL
{
    struct expected_returns * exp_returns = NULL;

    va_list ap;
    va_start(ap, Type);
    {
        struct value default_value = get_value_by_type(Type, ap);

        exp_returns = find_expected_returns(ExpectedReturns, Func);
        if (!exp_returns)
            return default_value;

        const char * param;
        while ((param = va_arg(ap, const char *)))
        {
            value_type_t type = va_arg(ap, value_type_t);
            assert(type == MEMORY); // This is the only one that makes sense
            void * ptr   = va_arg(ap, void *);
            size_t size  = va_arg(ap, size_t);

            struct param * exp_param = find_param(exp_returns->Params, param);
            if (!exp_param)
                continue;

            set_return_value(exp_param->Value, ptr, size);
        }
    }
    va_end(ap);

    struct value return_value = exp_returns->Value;

    if (exp_returns->When == WHEN_ONCE)
        remove_expected_returns(&ExpectedReturns, exp_returns);

    return return_value;
}

void
reset_expectations()
{
    struct expected_params * ep;
    while ((ep = ExpectedParams))
    {
        ExpectedParams = ExpectedParams->Next;

        if (ep->Params)
        {
            for (int i = 0; ep->Params[i]; i++)
                free(ep->Params[i]);
            free(ep->Params);
        }

        free(ep);
    }

    struct expected_returns * er;
    while ((er = ExpectedReturns))
    {
        ExpectedReturns = ExpectedReturns->Next;

        if (er->Params)
        {
            for (int i = 0; er->Params[i]; i++)
                free(er->Params[i]);
            free(er->Params);
        }

        free(er);
    }
}

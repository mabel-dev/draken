#pragma once
#include <stdint.h>
#include <stddef.h>

typedef enum {
    DRAKEN_INT8,
    DRAKEN_INT16,
    DRAKEN_INT32,
    DRAKEN_INT64,
    DRAKEN_FLOAT32,
    DRAKEN_FLOAT64,
    DRAKEN_DATE32,
    DRAKEN_TIMESTAMP64,
    DRAKEN_BOOL,
    DRAKEN_STRING,
    DRAKEN_ARRAY
} DrakenType;

typedef struct {
    void* data;               // int64_t*, double*, etc.
    uint8_t* null_bitmap;     // optional, 1 bit per row
    size_t length;
    size_t itemsize;
    DrakenType type;
} DrakenFixedBuffer;

typedef struct {
    uint8_t* data;            // UTF-8 bytes
    int32_t* offsets;         // [N+1] entries
    uint8_t* null_bitmap;     // optional
    size_t length;
} DrakenVarBuffer;

typedef struct {
    int32_t* offsets;         // [length + 1] entries
    void* values;             // pointer to another column's data (DrakenFixedColumn*, DrakenVarColumn*, etc.)
    uint8_t* null_bitmap;     // optional, 1 bit per row
    size_t length;            // number of array entries (rows)
    DrakenType value_type;    // type of the child values
} DrakenArrayBuffer;
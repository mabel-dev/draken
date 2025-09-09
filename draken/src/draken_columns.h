#pragma once
#include <stdint.h>
#include <stddef.h>

typedef enum {
    DRAKEN_INT64,
    DRAKEN_FLOAT64,
    DRAKEN_STRING,
    DRAKEN_BOOL
} DrakenType;

typedef struct {
    void* data;               // int64_t*, double*, etc.
    uint8_t* null_bitmap;     // optional, 1 bit per row
    size_t length;
    size_t itemsize;
    DrakenType type;
} DrakenFixedColumn;

typedef struct {
    uint8_t* data;            // UTF-8 bytes
    int32_t* offsets;         // [N+1] entries
    uint8_t* null_bitmap;     // optional
    size_t length;
} DrakenVarColumn;
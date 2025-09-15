"""
Draken buffer type declarations for fixed-width, variable-width, and array columns.

This header exposes C structs and enums for columnar data buffers used in Draken:

- DrakenType: Enum for supported data types (int, float, bool, string, array, etc.)
- DrakenFixedBuffer: Struct for fixed-width columns (e.g., int64, float64)
- DrakenVarBuffer: Struct for variable-width columns (e.g., UTF-8 strings)
- DrakenArrayBuffer: Struct for array/list columns (with offsets and child values)

These types are used to interface Cython and C code for efficient memory management and interoperability.
"""

from libc.stdint cimport int32_t
from libc.stdint cimport uint8_t

cdef extern from "buffers.h":

    ctypedef enum DrakenType:
        DRAKEN_INT8
        DRAKEN_INT16
        DRAKEN_INT32,
        DRAKEN_INT64
        DRAKEN_FLOAT32
        DRAKEN_FLOAT64
        DRAKEN_DATE32
        DRAKEN_TIMESTAMP64
        DRAKEN_BOOL
        DRAKEN_STRING
        DRAKEN_ARRAY

        DRAKEN_NON_NATIVE

    # Fixed-width column
    ctypedef struct DrakenFixedBuffer:
        void* data                 # int64_t*, double*, etc.
        uint8_t* null_bitmap       # optional, 1 bit per row
        size_t length
        size_t itemsize
        DrakenType type

    # Variable-width column (string/binary)
    ctypedef struct DrakenVarBuffer:
        uint8_t* data              # UTF-8 bytes
        int32_t* offsets           # [N+1] entries
        uint8_t* null_bitmap       # optional
        size_t length
        DrakenType type

    # Array column (list<T>)
    ctypedef struct DrakenArrayBuffer:
        int32_t* offsets           # [length + 1] entries
        void* values               # pointer to another column (DrakenFixedColumn*, DrakenVarColumn*, etc.)
        uint8_t* null_bitmap       # optional, 1 bit per row
        size_t length              # number of array entries (rows)
        DrakenType value_type      # type of the child values

    ctypedef struct DrakenMorsel:
        const char** column_names
        DrakenType* column_types
        void** columns
        size_t num_columns
        size_t num_rows

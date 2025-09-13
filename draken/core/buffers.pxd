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

    # Array column (list<T>)
    ctypedef struct DrakenArrayBuffer:
        int32_t* offsets           # [length + 1] entries
        void* values               # pointer to another column (DrakenFixedColumn*, DrakenVarColumn*, etc.)
        uint8_t* null_bitmap       # optional, 1 bit per row
        size_t length              # number of array entries (rows)
        DrakenType value_type      # type of the child values
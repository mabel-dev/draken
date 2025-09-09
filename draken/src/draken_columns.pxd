from libc.stdint cimport uint8_t, int32_t

cdef extern from "draken_columns.h":

    ctypedef enum DrakenType:
        DRAKEN_INT64
        DRAKEN_FLOAT64
        DRAKEN_STRING
        DRAKEN_BOOL

    ctypedef struct DrakenFixedColumn:
        void* data
        uint8_t* null_bitmap
        size_t length
        size_t itemsize
        DrakenType type

    ctypedef struct DrakenVarColumn:
        uint8_t* data
        int32_t* offsets
        uint8_t* null_bitmap
        size_t length
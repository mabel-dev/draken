from libc.stdint cimport uint8_t, int32_t, int64_t
from draken.src.draken_columns cimport DrakenFixedColumn
from draken.src.draken_columns cimport DrakenVarColumn

cdef struct DrakenStringView:
    const char* data
    int32_t length

cdef inline bint is_null(uint8_t* bitmap, size_t idx):
    if bitmap == NULL:
        return False
    return not (bitmap[idx >> 3] & (1 << (idx & 7)))

cdef inline int64_t get_int64(DrakenFixedColumn* col, size_t idx):
    if is_null(col.null_bitmap, idx):
        raise ValueError("NULL value")
    return (<int64_t*>col.data)[idx]

cdef inline double get_float64(DrakenFixedColumn* col, size_t idx):
    if is_null(col.null_bitmap, idx):
        raise ValueError("NULL value")
    return (<double*>col.data)[idx]

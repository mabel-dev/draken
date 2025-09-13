# cython: language_level=3
from libc.stdlib cimport malloc, free
from libc.stdint cimport uint8_t

from draken.core.buffers cimport DrakenFixedBuffer, DrakenType

cdef inline DrakenFixedBuffer* alloc_fixed_buffer(DrakenType dtype, size_t length, size_t itemsize):
    cdef DrakenFixedBuffer* buf = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if buf == NULL:
        raise MemoryError()
    buf.data = malloc(length * itemsize) if itemsize > 0 and length > 0 else NULL
    if length > 0 and itemsize > 0 and buf.data == NULL:
        free(buf)
        raise MemoryError()
    buf.null_bitmap = NULL
    buf.length = length
    buf.itemsize = itemsize
    buf.type = dtype
    return buf

cdef inline void free_fixed_buffer(DrakenFixedBuffer* buf, bint owns_data):
    if buf != NULL:
        if owns_data and buf.data != NULL:
            free(buf.data)
        if buf.null_bitmap != NULL:
            free(buf.null_bitmap)
        free(buf)

cdef inline size_t buf_length(DrakenFixedBuffer* buf):
    return buf.length

cdef inline size_t buf_itemsize(DrakenFixedBuffer* buf):
    return buf.itemsize

cdef inline int buf_dtype(DrakenFixedBuffer* buf):
    return <int>buf.type
# cython: language_level=3

from libc.stdint cimport int32_t, int8_t, intptr_t, uint64_t
from draken.core.buffers cimport DrakenVarBuffer
from draken.vectors.vector cimport Vector

cdef class StringVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_offs_buf
    cdef object _arrow_null_buf

    cdef DrakenVarBuffer* ptr
    cdef bint owns_data

    cpdef int8_t[::1] equals(self, bytes value)
    cpdef uint64_t[::1] hash(self)
    cpdef StringVector take(self, int32_t[::1] indices)

    cpdef list to_pylist(self)

cdef StringVector from_arrow(object array)
cdef StringVector from_arrow_struct(object array)

cpdef StringVector uppercase(StringVector input)
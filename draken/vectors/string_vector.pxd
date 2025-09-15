# cython: language_level=3

from libc.stdint cimport int32_t, int8_t, intptr_t, uint64_t
from draken.core.buffers cimport DrakenVarBuffer
from draken.vectors.vector cimport Vector

cdef class StringVector(Vector):
    cdef DrakenVarBuffer* ptr
    cdef bint owns_data

    cpdef bytes get(self, Py_ssize_t i)
    cpdef int8_t[::1] equals(self, bytes value)
    cpdef uint64_t[::1] hash(self)

cdef StringVector from_arrow(object array)
# cython: language_level=3

from libc.stdint cimport int32_t, int8_t, intptr_t, uint64_t, uint8_t
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
    cpdef Py_ssize_t byte_length(self, Py_ssize_t i)
    cpdef object buffers(self)
    cpdef object null_bitmap(self)
    cpdef int32_t[::1] lengths(self)
    cpdef object view(self)

cdef class _StringVectorView:
    cdef DrakenVarBuffer* _ptr
    cdef char* _data
    cdef int32_t* _offsets
    cdef uint8_t* _nulls

    cpdef intptr_t value_ptr(self, Py_ssize_t i)
    cpdef Py_ssize_t value_len(self, Py_ssize_t i)
    cpdef bint is_null(self, Py_ssize_t i)

cdef StringVector from_arrow(object array)
cdef StringVector from_arrow_struct(object array)

cpdef StringVector uppercase(StringVector input)
# draken/vectors/bool_vector.pxd

# cython: language_level=3

from libc.stdint cimport int32_t, int8_t, uint64_t
from draken.core.buffers cimport DrakenFixedBuffer
from draken.vectors.vector cimport Vector

cdef class BoolVector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    # Ops
    cpdef BoolVector take(self, int32_t[::1] indices)
    cpdef int8_t[::1] equals(self, bint value)
    cpdef int8_t[::1] not_equals(self, bint value)
    cpdef int8_t any(self)
    cpdef int8_t all(self)
    cpdef int8_t[::1] is_null(self)
    cpdef list to_pylist(self)
    cpdef uint64_t[::1] hash(self)

cdef BoolVector from_arrow(object array)
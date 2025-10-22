from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t
from libc.stdint cimport uint64_t

from draken.core.buffers cimport DrakenFixedBuffer
from draken.vectors.vector cimport Vector

cdef class Int64Vector(Vector):
    cdef object _arrow_data_buf
    cdef object _arrow_null_buf
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    cpdef Int64Vector take(self, int32_t[::1] indices)

    cpdef int8_t[::1] equals(self, int64_t value)
    cpdef int8_t[::1] equals_vector(self, Int64Vector other)
    cpdef int8_t[::1] not_equals(self, int64_t value)
    cpdef int8_t[::1] not_equals_vector(self, Int64Vector other)
    cpdef int8_t[::1] greater_than(self, int64_t value)
    cpdef int8_t[::1] greater_than_vector(self, Int64Vector other)
    cpdef int8_t[::1] greater_than_or_equals(self, int64_t value)
    cpdef int8_t[::1] greater_than_or_equals_vector(self, Int64Vector other)
    cpdef int8_t[::1] less_than(self, int64_t value)
    cpdef int8_t[::1] less_than_vector(self, Int64Vector other)
    cpdef int8_t[::1] less_than_or_equals(self, int64_t value)
    cpdef int8_t[::1] less_than_or_equals_vector(self, Int64Vector other)

    cpdef int8_t[::1] is_null(self)

    cpdef list to_pylist(self)
    #cpdef int64_t __getitem__(self, Py_ssize_t i)

    cpdef int64_t sum(self)
    cpdef int64_t min(self)
    cpdef int64_t max(self)

    cpdef uint64_t[::1] hash(self)

cdef Int64Vector from_arrow(object array)
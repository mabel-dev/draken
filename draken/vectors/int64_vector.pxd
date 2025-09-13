from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport int8_t

from draken.core.buffers cimport DrakenFixedBuffer

cdef class Int64Vector:
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    cpdef Int64Vector take(self, int32_t[::1] indices)
    cpdef int8_t[::1] compare(self, int64_t value)

cdef Int64Vector from_arrow(object array)
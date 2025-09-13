from libc.stdint cimport int8_t, int32_t, int64_t
from draken.core.buffers cimport DrakenFixedBuffer

cdef class Int64Vector:
    cdef DrakenFixedBuffer* ptr
    cdef bint owns_data

    cpdef Int64Vector take(self, int32_t[::1] indices)
    cpdef int8_t[::1] compare(self, int64_t value)
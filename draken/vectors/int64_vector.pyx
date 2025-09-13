# cython: language_level=3
# cython: boundscheck=False, wraparound=False, cdivision=True

import pyarrow
import ctypes

from libc.stdint cimport int64_t, int32_t, intptr_t, int8_t, uint8_t

from libc.stdlib cimport malloc, free
from cpython.mem cimport PyMem_Malloc, PyMem_Free
cimport cython

from draken.core.buffers cimport DrakenFixedBuffer, DRAKEN_INT64
from draken.core.fixed_vector cimport (
    alloc_fixed_buffer, free_fixed_buffer,
    buf_length, buf_itemsize, buf_dtype
)

cdef class Int64Vector:

    def __cinit__(self, size_t length=0, bint wrap=False):
        """
        length>0, wrap=False  -> allocate new owned buffer
        wrap=True             -> do not allocate; caller will set ptr & metadata
        """
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_fixed_buffer(DRAKEN_INT64, length, 8)
            self.owns_data = True

    def __dealloc__(self):
        free_fixed_buffer(self.ptr, self.owns_data)

    # Python-friendly properties (backed by C getters for kernels)
    @property
    def length(self):
        return buf_length(self.ptr)

    @property
    def itemsize(self):
        return buf_itemsize(self.ptr)

    @property
    def dtype(self):
        return buf_dtype(self.ptr)

    # -------- Interop (owned -> Arrow) --------
    def to_arrow(self):
        cdef size_t nbytes = buf_length(self.ptr) * buf_itemsize(self.ptr)
        addr = <intptr_t> self.ptr.data
        data_ptr = ctypes.cast(int(addr), ctypes.POINTER(ctypes.c_int64))
        data_buf = pyarrow.foreign_buffer(ctypes.addressof(data_ptr.contents), nbytes)
        # nulls not wired here; add when you have null_bitmap semantics
        return pyarrow.Array.from_buffers(pyarrow.int64(), buf_length(self.ptr), [None, data_buf])

    # -------- Example op --------
    cpdef Int64Vector take(self, int32_t[::1] indices):
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef Int64Vector out = Int64Vector(<size_t>n)
        cdef int64_t* src = <int64_t*> self.ptr.data
        cdef int64_t* dst = <int64_t*> out.ptr.data
        for i in range(n):
            dst[i] = src[indices[i]]
        return out

    cpdef int8_t[::1] compare(self, int64_t value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef int64_t* data = <int64_t*> ptr.data
        cdef Py_ssize_t i, n = ptr.length

        # Allocate n bytes using Python’s allocator
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()

        # Tight C loop
        for i in range(n):
            buf[i] = 1 if data[i] == value else 0

        # Expose as memoryview (takes ownership of the buffer)
        return <int8_t[:n]> buf

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        cdef int64_t* data = <int64_t*> self.ptr.data
        for i in range(k):
            vals.append(data[i])
        return f"<Int64Vector len={buf_length(self.ptr)} values={vals}>"
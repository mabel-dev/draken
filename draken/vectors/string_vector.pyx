# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
StringVector: Cython implementation of a variable-width byte column for Draken.

This module provides:
- The StringVector class for efficient byte/variable-length storage
- Integration with DrakenVarBuffer and helpers for memory management
- Arrow interoperability (zero-copy wrapping)
- Fast equality, null handling, and hashing
"""

import pyarrow

from cpython.mem cimport PyMem_Malloc
from libc.stdint cimport int32_t, uint8_t, intptr_t, uint64_t
from libc.stdlib cimport malloc
from libc.string cimport memcmp

from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DRAKEN_STRING
from draken.core.var_vector cimport alloc_var_buffer, free_var_buffer, buf_dtype
from draken.vectors.vector cimport Vector

# Constant for null hashes
cdef uint64_t NULL_HASH = <uint64_t>0x9e3779b97f4a7c15


cdef class StringVector(Vector):

    def __cinit__(self, size_t length=0, size_t bytes_cap=0, bint wrap=False):
        """
        length>0, wrap=False  -> allocate new owned buffer
        wrap=True             -> no allocation; caller will set ptr
        """
        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            self.ptr = alloc_var_buffer(DRAKEN_STRING, length, bytes_cap)
            self.owns_data = True

    def __dealloc__(self):
        if self.owns_data and self.ptr is not NULL:
            free_var_buffer(self.ptr, True)
            self.ptr = NULL

    @property
    def dtype(self):
        return buf_dtype(self.ptr)

    def to_arrow(self):
        """
        Zero-copy conversion to Arrow StringArray
        """
        cdef size_t n = self.ptr.length
        data_buf = pyarrow.foreign_buffer(<intptr_t>self.ptr.data,
                                          self.ptr.offsets[n])
        offs_buf = pyarrow.foreign_buffer(<intptr_t>self.ptr.offsets,
                                          (n + 1) * sizeof(int32_t))
        return pyarrow.Array.from_buffers(pyarrow.string(), n,
                                          [None, offs_buf, data_buf])

    cpdef bytes get(self, Py_ssize_t i):
        """
        Return entry i as raw bytes.
        """
        cdef DrakenVarBuffer* ptr = self.ptr
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of range")
        cdef int32_t start = ptr.offsets[i]
        cdef int32_t end = ptr.offsets[i+1]
        cdef Py_ssize_t nbytes = end - start
        return (<char*>ptr.data + start)[:nbytes]

    cpdef int8_t[::1] equals(self, bytes value):
        """
        Return mask: 1 if equal to value, else 0.
        """
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()

        cdef char* val_ptr = value
        cdef Py_ssize_t val_len = len(value)
        cdef int32_t start, end, can_len
        cdef int i

        for i in range(n):
            start = ptr.offsets[i]
            end = ptr.offsets[i+1]
            can_len = end - start
            if can_len == val_len and memcmp(<char*>ptr.data + start, val_ptr, can_len) == 0:
                buf[i] = 1
            else:
                buf[i] = 0

        return <int8_t[:n]> buf

    cpdef uint64_t[::1] hash(self):
        """
        Produce lightweight 64-bit hashes from byte sequences.
        """
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef uint64_t* buf = <uint64_t*> PyMem_Malloc(n * sizeof(uint64_t))
        if buf == NULL:
            raise MemoryError()

        cdef int32_t start, end
        cdef uint64_t h
        cdef uint8_t* p
        cdef Py_ssize_t j, can_len
        for j in range(n):
            if ptr.null_bitmap != NULL:
                if (ptr.null_bitmap[j >> 3] >> (j & 7)) & 1 == 0:
                    buf[j] = NULL_HASH
                    continue

            start = ptr.offsets[j]
            end = ptr.offsets[j+1]
            can_len = end - start
            p = <uint8_t*>ptr.data + start

            # simple xor-shift hash
            h = 0xcbf29ce484222325
            for i in range(can_len):
                h ^= p[i]
                h *= 0x100000001b3
            buf[j] = h

        return <uint64_t[:n]> buf

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(self.ptr.length, 5)
        for i in range(k):
            vals.append(self.get(i))
        return f"<StringVector len={self.ptr.length} values={vals}>"


cdef StringVector from_arrow(object array):
    """
    Wrap an Arrow StringArray without copying.
    """
    cdef StringVector vec = StringVector(0, 0, True)
    vec.ptr = <DrakenVarBuffer*> malloc(sizeof(DrakenVarBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    vec.ptr.length = <size_t> len(array)

    # Data buffer (bytes)
    cdef intptr_t data_addr = <intptr_t> bufs[2].address
    vec.ptr.data = <uint8_t*> data_addr

    # Offsets buffer (int32_t[length+1])
    cdef intptr_t offs_addr = <intptr_t> bufs[1].address
    vec.ptr.offsets = <int32_t*> offs_addr

    # Null bitmap (optional)
    cdef intptr_t nb_addr
    if bufs[0] is not None:
        nb_addr = <intptr_t> bufs[0].address
        vec.ptr.null_bitmap = <uint8_t*> nb_addr
    else:
        vec.ptr.null_bitmap = NULL

    vec.ptr.type = DRAKEN_STRING
    return vec

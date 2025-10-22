# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
BoolVector: Cython implementation of a zero-copy, bit-packed boolean column vector for Draken.

This matches Arrow's representation:
- Values are stored bit-packed in data buffer (1 bit per value).
- Nulls are stored in the null_bitmap (same layout).
- Zero-copy interop with Arrow via from_arrow/to_arrow.

"""

import ctypes
import pyarrow

from cpython.mem cimport PyMem_Malloc

from libc.stdint cimport int32_t, int8_t, intptr_t, uint64_t, uint8_t
from libc.stdlib cimport malloc

from draken.core.buffers cimport DrakenFixedBuffer
from draken.core.buffers cimport DRAKEN_BOOL
from draken.core.fixed_vector cimport alloc_fixed_buffer, buf_dtype, buf_length, free_fixed_buffer
from draken.vectors.vector cimport Vector

# NULL_HASH constant for null hash entries
cdef uint64_t NULL_HASH = <uint64_t>0x9e3779b97f4a7c15

cdef class BoolVector(Vector):

    def __cinit__(self, size_t length=0, bint wrap=False):
        cdef size_t nbytes

        if wrap:
            self.ptr = NULL
            self.owns_data = False
        else:
            # bit-packed, so allocate ceil(length/8) bytes
            nbytes = (length + 7) >> 3
            self.ptr = alloc_fixed_buffer(DRAKEN_BOOL, length, 1)  # itemsize=1 is logical
            if self.ptr != NULL:
                self.ptr.data = PyMem_Malloc(nbytes)
                if self.ptr.data == NULL:
                    raise MemoryError()
            self.owns_data = True

    def __dealloc__(self):
        if self.owns_data and self.ptr is not NULL:
            free_fixed_buffer(self.ptr, True)
            self.ptr = NULL

    # Properties
    @property
    def length(self):
        return buf_length(self.ptr)

    @property
    def dtype(self):
        return buf_dtype(self.ptr)

    def __getitem__(self, Py_ssize_t i) -> bool:
        cdef DrakenFixedBuffer* ptr = self.ptr
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of bounds")
        # null check
        if ptr.null_bitmap != NULL:
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                raise ValueError(f"Value at index {i} is null")
        # extract bit
        cdef uint8_t val_byte = (<uint8_t*>ptr.data)[i >> 3]
        return bool((val_byte >> (i & 7)) & 1)

    # -------- Interop --------
    def to_arrow(self):
        # Wrap existing bit-packed buffer
        cdef size_t nbytes = (buf_length(self.ptr) + 7) >> 3
        addr = <intptr_t> self.ptr.data
        data_ptr = ctypes.cast(int(addr), ctypes.POINTER(ctypes.c_uint8))
        data_buf = pyarrow.foreign_buffer(ctypes.addressof(data_ptr.contents), nbytes)

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(pyarrow.foreign_buffer(<intptr_t> self.ptr.null_bitmap, (self.ptr.length + 7) // 8))
        else:
            buffers.append(None)

        buffers.append(data_buf)

        return pyarrow.Array.from_buffers(pyarrow.bool_(), buf_length(self.ptr), buffers)

    # -------- Ops --------
    cpdef BoolVector take(self, int32_t[::1] indices):
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef BoolVector out = BoolVector(<size_t>n)
        cdef uint8_t* src = <uint8_t*> self.ptr.data
        cdef uint8_t* dst = <uint8_t*> out.ptr.data
        # zero init
        cdef Py_ssize_t out_nbytes = (n + 7) >> 3
        for i in range(out_nbytes):
            dst[i] = 0
        for i in range(n):
            idx = indices[i]
            if ((src[idx >> 3] >> (idx & 7)) & 1) != 0:
                dst[i >> 3] |= (1 << (i & 7))
        return out

    cpdef int8_t[::1] equals(self, bint value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        cdef int target = 1 if value else 0
        for i in range(n):
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            buf[i] = 1 if val == target else 0
        return <int8_t[:n]> buf

    cpdef int8_t[::1] not_equals(self, bint value):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        if buf == NULL:
            raise MemoryError()
        cdef int target = 1 if value else 0
        for i in range(n):
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            buf[i] = 1 if val != target else 0
        return <int8_t[:n]> buf

    cpdef int8_t any(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t nbytes = (ptr.length + 7) >> 3
        cdef Py_ssize_t i
        for i in range(nbytes):
            if (<uint8_t*>ptr.data)[i] != 0:
                return 1
        return 0

    cpdef int8_t all(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t n = ptr.length
        cdef Py_ssize_t i
        for i in range(n):
            if (((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1) == 0:
                return 0
        return 1

    cpdef int8_t[::1] is_null(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef int8_t* buf = <int8_t*> PyMem_Malloc(n)
        cdef uint8_t byte, bit
        if buf == NULL:
            raise MemoryError()
        if ptr.null_bitmap == NULL:
            for i in range(n):
                buf[i] = 0
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                buf[i] = 0 if bit else 1
        return <int8_t[:n]> buf

    @property
    def null_count(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef Py_ssize_t count = 0
        cdef uint8_t byte, bit
        if ptr.null_bitmap == NULL:
            return 0
        for i in range(n):
            byte = ptr.null_bitmap[i >> 3]
            bit = (byte >> (i & 7)) & 1
            if not bit:
                count += 1
        return count

    cpdef list to_pylist(self):
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef uint8_t byte, bit
        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    out.append(None)
                    continue
            val = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            out.append(bool(val))
        return out

    cpdef uint64_t[::1] hash(self):
        """
        Produce lightweight 64-bit hashes from bit-packed boolean data.
        Map False->0, True->1. Nulls -> NULL_HASH.
        """
        cdef DrakenFixedBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef uint64_t* buf = <uint64_t*> PyMem_Malloc(n * sizeof(uint64_t))
        if buf == NULL:
            raise MemoryError()

        cdef uint64_t x
        cdef uint8_t byte, bit
        for i in range(n):
            if ptr.null_bitmap != NULL:
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    buf[i] = NULL_HASH
                    continue
            x = ((<uint8_t*>ptr.data)[i >> 3] >> (i & 7)) & 1
            buf[i] = (x ^ (x >> 33)) * <uint64_t>0xff51afd7ed558ccdU
        return <uint64_t[:n]> buf

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(<Py_ssize_t>buf_length(self.ptr), 10)
        for i in range(k):
            vals.append(bool(((<uint8_t*>self.ptr.data)[i >> 3] >> (i & 7)) & 1))
        return f"<BoolVector len={buf_length(self.ptr)} values={vals}>"


cdef BoolVector from_arrow(object array):
    cdef BoolVector vec = BoolVector(0, True)
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    # Keep references to prevent GC
    vec._arrow_null_buf = bufs[0]
    vec._arrow_data_buf = bufs[1]

    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr

    vec.ptr.type = DRAKEN_BOOL
    vec.ptr.itemsize = 1
    vec.ptr.length = <size_t> len(array)

    # Arrow boolean buffer is bit-packed already
    vec.ptr.data = <void*> (base_ptr + (offset >> 3))

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        vec.ptr.null_bitmap = <uint8_t*> nb_addr
    else:
        vec.ptr.null_bitmap = NULL

    return vec

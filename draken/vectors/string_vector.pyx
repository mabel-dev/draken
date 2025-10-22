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
from cpython.bytes cimport PyBytes_AS_STRING
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int32_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint64_t
from libc.string cimport memcpy, memset, memcmp
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libc.stdlib cimport malloc
from libc.string cimport memcmp
from libc.string cimport memcpy

from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DRAKEN_STRING
from draken.core.var_vector cimport alloc_var_buffer
from draken.core.var_vector cimport buf_dtype
from draken.core.var_vector cimport free_var_buffer
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
    def length(self):
        """Return the number of elements in the vector."""
        return self.ptr.length

    @property
    def dtype(self):
        return buf_dtype(self.ptr)

    def to_arrow(self):
        """
        Zero-copy conversion to Arrow StringArray (bytes-based).
        """
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef size_t n = ptr.length

        # Data buffer: all the concatenated string bytes
        data_buf = pyarrow.foreign_buffer(<intptr_t>ptr.data, ptr.offsets[n])

        # Offsets buffer: (n+1) * int32_t entries
        offs_buf = pyarrow.foreign_buffer(<intptr_t>ptr.offsets, (n + 1) * sizeof(int32_t))

        # Null bitmap buffer (optional)
        if ptr.null_bitmap != NULL:
            null_buf = pyarrow.foreign_buffer(<intptr_t>ptr.null_bitmap, (n + 7) // 8)
        else:
            null_buf = None

        return pyarrow.Array.from_buffers(pyarrow.binary(), n, [null_buf, offs_buf, data_buf])

    def __getitem__(self, Py_ssize_t i) -> bytes:
        """
        Return entry i as raw bytes.
        """
        cdef DrakenVarBuffer* ptr = self.ptr
        if i < 0 or i >= ptr.length:
            raise IndexError("Index out of range")

        cdef int32_t start = ptr.offsets[i]
        cdef int32_t end = ptr.offsets[i+1]
        cdef Py_ssize_t nbytes = end - start
        cdef char* base = <char*>ptr.data
        return PyBytes_FromStringAndSize(base + start, nbytes)

    @property
    def null_count(self):
        """Return the number of nulls in the vector."""
        cdef DrakenVarBuffer* ptr = self.ptr
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

    cpdef list to_pylist(self):
        """
        Convert the StringVector into a Python list of str or None (for nulls).
        """
        cdef DrakenVarBuffer* ptr = self.ptr
        cdef Py_ssize_t i, n = ptr.length
        cdef list out = []
        cdef int32_t start, end, nbytes
        cdef char* base = <char*>ptr.data
        cdef uint8_t byte, bit

        if ptr.null_bitmap == NULL:
            for i in range(n):
                start = ptr.offsets[i]
                end = ptr.offsets[i+1]
                nbytes = end - start
                out.append((<bytes>PyBytes_FromStringAndSize(base + start, nbytes)).decode("utf8"))
        else:
            for i in range(n):
                byte = ptr.null_bitmap[i >> 3]
                bit = (byte >> (i & 7)) & 1
                if not bit:
                    out.append(None)
                else:
                    start = ptr.offsets[i]
                    end = ptr.offsets[i+1]
                    nbytes = end - start
                    out.append((<bytes>PyBytes_FromStringAndSize(base + start, nbytes)).decode("utf8"))
        return out

    cpdef StringVector take(self, int32_t[::1] indices):
        """
        Take rows by indices - high performance implementation for StringVector.
        """
        cdef Py_ssize_t i, n = indices.shape[0]
        cdef DrakenVarBuffer* src_ptr = self.ptr
        cdef int32_t src_idx, start, end, byte_len
        cdef int32_t total_bytes = 0

        # First pass: calculate total bytes needed
        for i in range(n):
            src_idx = indices[i]
            if src_idx < 0 or src_idx >= src_ptr.length:
                raise IndexError(f"Index {src_idx} out of bounds for length {src_ptr.length}")
            start = src_ptr.offsets[src_idx]
            end = src_ptr.offsets[src_idx + 1]
            total_bytes += (end - start)

        # Create result vector
        cdef StringVector result = StringVector(n, total_bytes)
        cdef DrakenVarBuffer* dst_ptr = result.ptr

        # Copy data
        cdef char* src_data = <char*>src_ptr.data
        cdef char* dst_data = <char*>dst_ptr.data
        cdef int32_t* dst_offsets = dst_ptr.offsets
        cdef int32_t dst_offset = 0
        cdef uint8_t src_bit, dst_byte_idx, dst_bit_idx

        dst_offsets[0] = 0

        for i in range(n):
            src_idx = indices[i]
            start = src_ptr.offsets[src_idx]
            end = src_ptr.offsets[src_idx + 1]
            byte_len = end - start

            # Copy string data
            if byte_len > 0:
                memcpy(dst_data + dst_offset, src_data + start, byte_len)

            dst_offset += byte_len
            dst_offsets[i + 1] = dst_offset

            # Handle null bitmap if present
            if src_ptr.null_bitmap != NULL:
                if dst_ptr.null_bitmap == NULL:
                    # Allocate null bitmap for destination
                    dst_ptr.null_bitmap = <uint8_t*>PyMem_Malloc((n + 7) // 8)
                    if dst_ptr.null_bitmap == NULL:
                        raise MemoryError()
                    # Initialize to all valid (1s)
                    memset(dst_ptr.null_bitmap, 0xFF, (n + 7) // 8)

                # Copy null bit
                src_bit = (src_ptr.null_bitmap[src_idx >> 3] >> (src_idx & 7)) & 1
                dst_byte_idx = i >> 3
                dst_bit_idx = i & 7

                if src_bit:
                    dst_ptr.null_bitmap[dst_byte_idx] |= (1 << dst_bit_idx)
                else:
                    dst_ptr.null_bitmap[dst_byte_idx] &= ~(1 << dst_bit_idx)

        return result

    def __str__(self):
        cdef list vals = []
        cdef Py_ssize_t i, k = min(self.ptr.length, 5)
        for i in range(k):
            vals.append(self[i])
        return f"<StringVector len={self.ptr.length} values={vals}>"


cdef StringVector from_arrow(object array):
    """
    Wrap an Arrow StringArray without copying.
    Keeps references to Arrow buffers to prevent GC from freeing memory.
    """
    cdef StringVector vec = StringVector(0, 0, True)
    vec.ptr = <DrakenVarBuffer*> malloc(sizeof(DrakenVarBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    vec._arrow_null_buf = bufs[0]
    vec._arrow_offs_buf = bufs[1]
    vec._arrow_data_buf = bufs[2]

    vec.ptr.length = <size_t> len(array)

    # Data buffer (bytes)
    cdef intptr_t data_addr = bufs[2].address
    vec.ptr.data = <uint8_t*> data_addr

    # Offsets buffer (int32_t[length+1])
    cdef intptr_t offs_addr = bufs[1].address
    vec.ptr.offsets = <int32_t*> offs_addr

    # Null bitmap (optional)
    cdef intptr_t nb_addr
    if bufs[0] is not None:
        nb_addr = bufs[0].address
        vec.ptr.null_bitmap = <uint8_t*> nb_addr
    else:
        vec.ptr.null_bitmap = NULL

    vec.ptr.type = DRAKEN_STRING
    return vec

cdef inline bint is_null(uint8_t* bitmap, Py_ssize_t i):
    """Check if row i is null, given Arrow-style bitmap (1=valid, 0=null)."""
    if bitmap == NULL:
        return False
    return not ((bitmap[i >> 3] >> (i & 7)) & 1)

cdef StringVector from_arrow_struct(object array):
    """
    Convert an Arrow StructArray into a StringVector of JSON strings.
    Each row becomes {"field": value, ...}
    """
    cdef Py_ssize_t n = len(array)
    cdef list field_names = [f.name for f in array.type]
    cdef int nfields = len(field_names)
    cdef Py_ssize_t nb_size

    # crude capacity guess: 64 bytes per row
    cdef StringVector vec = StringVector(n, n * 64, False)
    vec.owns_data = True
    cdef DrakenVarBuffer* ptr = vec.ptr

    cdef object bufs = array.buffers()
    cdef intptr_t nb_addr
    cdef uint8_t* parent_null_bitmap = NULL
    if bufs[0] is not None:
        nb_addr = bufs[0].address
        parent_null_bitmap = <uint8_t*> nb_addr

        # allocate and copy null bitmap into Draken
        nb_size = (n + 7) // 8
        ptr.null_bitmap = <uint8_t*> malloc(nb_size)
        if ptr.null_bitmap == NULL:
            raise MemoryError()
        memcpy(ptr.null_bitmap, parent_null_bitmap, nb_size)
    else:
        ptr.null_bitmap = NULL

    cdef Py_ssize_t offset = 0
    cdef Py_ssize_t i, j
    cdef bytes json_bytes
    cdef const char* jb_ptr

    ptr.offsets[0] = 0

    for i in range(n):
        if is_null(parent_null_bitmap, i):
            # just carry forward same offset (null row = empty string)
            ptr.offsets[i+1] = offset
            continue

        # build JSON row as Python string for now
        row_items = []
        for j in range(nfields):
            val = array.field(j)[i].as_py()
            if val is None:
                row_items.append(f'"{field_names[j]}": null')
            elif isinstance(val, str):
                # naive escaping
                row_items.append(f'"{field_names[j]}": "{val}"')
            else:
                row_items.append(f'"{field_names[j]}": {val}')
        json_str = "{" + ",".join(row_items) + "}"
        json_bytes = json_str.encode("utf8")

        jb_ptr = PyBytes_AS_STRING(json_bytes)
        memcpy(<char*>ptr.data + offset, jb_ptr, len(json_bytes))

        offset += len(json_bytes)
        ptr.offsets[i+1] = offset

    return vec

#################################

cpdef StringVector uppercase(StringVector input):
    """
    Return a new StringVector with all non-null values uppercased.
    """
    cdef DrakenVarBuffer* in_ptr = input.ptr
    cdef Py_ssize_t i, n = in_ptr.length
    cdef int32_t start, end, length

    # Estimate total bytes (uppercased values won't be longer)
    cdef int32_t total_bytes = in_ptr.offsets[n]

    # Allocate new buffer
    cdef StringVector result = StringVector(n, total_bytes)
    cdef DrakenVarBuffer* out_ptr = result.ptr

    cdef char* in_data = <char*>in_ptr.data
    cdef char* out_data = <char*>out_ptr.data
    cdef int32_t* out_offsets = out_ptr.offsets
    cdef int32_t offset = 0
    out_offsets[0] = 0

    cdef char* src
    cdef char ch
    cdef int j

    for i in range(n):
        if in_ptr.null_bitmap != NULL and ((in_ptr.null_bitmap[i >> 3] >> (i & 7)) & 1) == 0:
            # Set null bit
            if out_ptr.null_bitmap == NULL:
                out_ptr.null_bitmap = <uint8_t*> malloc((n + 7) // 8)
                for j in range((n + 7) // 8):
                    out_ptr.null_bitmap[j] = 0xFF  # Initially mark all as valid

            out_ptr.null_bitmap[i >> 3] &= ~(1 << (i & 7))  # Mark as null
            out_offsets[i + 1] = offset
            continue

        # Get string bounds
        start = in_ptr.offsets[i]
        end = in_ptr.offsets[i + 1]
        length = end - start
        src = in_data + start

        for j in range(length):
            ch = src[j]
            if 97 <= ch <= 122:  # 'a'..'z'
                out_data[offset + j] = ch - 32
            else:
                out_data[offset + j] = ch

        offset += length
        out_offsets[i + 1] = offset

    return result

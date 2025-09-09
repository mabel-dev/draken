# draken.pyx

import pyarrow
import ctypes

from libc.stdlib cimport free
from libc.stdint cimport int8_t, uint8_t, int32_t, int64_t, intptr_t
from libc.stdlib cimport malloc, calloc
from libc.string cimport memset

from draken.src.draken_columns cimport DrakenFixedColumn
from draken.src.draken_columns cimport DrakenVarColumn
from draken.src.draken_columns cimport DrakenType
from draken.src.draken_columns cimport DRAKEN_INT64
from draken.src.draken_columns cimport DRAKEN_FLOAT64
from draken.src.draken_columns cimport DRAKEN_BOOL
from draken.src.draken_columns cimport DRAKEN_STRING


cdef DrakenFixedColumn* alloc_fixed_column(DrakenType dtype, size_t length):
    cdef DrakenFixedColumn* col = <DrakenFixedColumn*> malloc(sizeof(DrakenFixedColumn))
    cdef size_t itemsize = 8 if dtype in [DRAKEN_INT64, DRAKEN_FLOAT64] else 1
    col.data = malloc(length * itemsize)
    col.null_bitmap = NULL
    col.length = length
    col.itemsize = itemsize
    col.type = dtype
    return col

cdef DrakenVarColumn* alloc_var_column(size_t length, size_t data_capacity):
    cdef DrakenVarColumn* col = <DrakenVarColumn*> malloc(sizeof(DrakenVarColumn))
    col.data = <uint8_t*> malloc(data_capacity)
    col.offsets = <int32_t*> calloc(length + 1, sizeof(int32_t))
    col.null_bitmap = NULL
    col.length = length
    return col

cpdef object py_alloc_fixed_column(int dtype, size_t length):
    cdef DrakenFixedColumn* col = alloc_fixed_column(<DrakenType>dtype, length)
    return <intptr_t>col  # Pass back the pointer as an integer (or wrap properly)

cpdef object py_alloc_var_column(size_t length, size_t data_capacity):
    cdef DrakenVarColumn* col = alloc_var_column(length, data_capacity)
    return <intptr_t>col  # Again, return raw pointer as int


cdef class FixedColumn:
    cdef DrakenFixedColumn* ptr
    cdef bint owns_data

    def __cinit__(self, int dtype, size_t length):
        self.ptr = alloc_fixed_column(<DrakenType>dtype, length)
        self.owns_data = True

    def __dealloc__(self):
        if self.ptr is not NULL:
            if self.owns_data and self.ptr.data != NULL:
                free(self.ptr.data)
            if self.ptr.null_bitmap != NULL:
                free(self.ptr.null_bitmap)
            free(self.ptr)
            self.ptr = NULL

    @property
    def length(self):
        return self.ptr.length

    @property
    def itemsize(self):
        return self.ptr.itemsize

    @property
    def dtype(self):
        return self.ptr.type

    cpdef intptr_t data_ptr(self):
        return <intptr_t>self.ptr.data

    cpdef FixedColumn take(self, int32_t[::1] indices):
        cdef:
            Py_ssize_t i, idx, n = len(indices)
            FixedColumn out = FixedColumn(self.dtype, n)
            int64_t* src = <int64_t*>self.ptr.data
            int64_t* dst = <int64_t*>out.ptr.data

        for i in range(n):
            idx = indices[i]
            dst[i] = src[idx]

        return out


    def to_arrow(self):

        if self.dtype != DRAKEN_INT64:
            raise NotImplementedError("Only DRAKEN_INT64 supported in to_arrow() for now.")

        addr = int(self.data_ptr())
        # Cast data pointer to C pointer
        data_ptr = ctypes.cast(addr, ctypes.POINTER(ctypes.c_int64))
        data_buf = pyarrow.foreign_buffer(ctypes.addressof(data_ptr.contents), self.length * self.itemsize)

        null_buf = None

        # Construct Arrow Array
        return pyarrow.Array.from_buffers(
            pyarrow.int64(),  # type
            self.length,
            [null_buf, data_buf]
        )

    def __str__(self):
        if self.ptr is NULL or self.ptr.data is NULL:
            return "<FixedColumn NULL>"

        cdef int64_t* i64_ptr
        cdef double* f64_ptr
        cdef list values = []
        cdef Py_ssize_t i
        cdef Py_ssize_t count = min(self.ptr.length, 10)

        if self.ptr.type == DRAKEN_INT64:
            i64_ptr = <int64_t*>self.ptr.data
            for i in range(count):
                values.append(i64_ptr[i])
            return f"<FixedColumn[int64] len={self.ptr.length} values={values}>"

        elif self.ptr.type == DRAKEN_FLOAT64:
            f64_ptr = <double*>self.ptr.data
            for i in range(count):
                values.append(f64_ptr[i])
            return f"<FixedColumn[float64] len={self.ptr.length} values={values}>"

        else:
            return f"<FixedColumn[type={self.ptr.type}] len={self.ptr.length}>"

cdef class VarColumn:
    cdef DrakenVarColumn* ptr

    def __cinit__(self, size_t length, size_t capacity):
        self.ptr = alloc_var_column(length, capacity)

    def __dealloc__(self):
        if self.ptr is not NULL:
            if self.ptr.data != NULL:
                free(self.ptr.data)
            if self.ptr.offsets != NULL:
                free(self.ptr.offsets)
            if self.ptr.null_bitmap != NULL:
                free(self.ptr.null_bitmap)
            free(self.ptr)
            self.ptr = NULL

    @property
    def length(self):
        return self.ptr.length


cpdef int INT64():
    return DRAKEN_INT64

cpdef int FLOAT64():
    return DRAKEN_FLOAT64

cpdef int STRING():
    return DRAKEN_STRING

cpdef int BOOL():
    return DRAKEN_BOOL

cpdef FixedColumn from_arrow_fixed(object array):
    """
    Create a FixedColumn from a PyArrow array (zero-copy).
    Only supports int64 for now.
    """
    cdef FixedColumn col = FixedColumn(0, 0)  # temp init
    cdef object buffers = array.buffers()
    cdef Py_ssize_t offset = array.offset
    cdef Py_ssize_t itemsize = 8  # for int64
    cdef intptr_t base_ptr = buffers[1].address

    col.ptr.length = len(array)
    col.ptr.itemsize = itemsize
    col.ptr.type = DRAKEN_INT64
    col.ptr.data = <void*> (base_ptr + offset * itemsize)

    if buffers[0] is not None:
        col.ptr.null_bitmap = <uint8_t*>buffers[0].address
    else:
        col.ptr.null_bitmap = NULL

    col.owns_data = False
    return col


import array

cpdef int8_t[::1] compare(FixedColumn col, int64_t value):
    cdef:
        DrakenFixedColumn* ptr = (<FixedColumn>col).ptr
        int64_t* data = <int64_t*>ptr.data
        Py_ssize_t i, n = ptr.length
        int8_t* result_buf = <int8_t*>malloc(n)
    
    for i in range(n):
        result_buf[i] = data[i] == value

    # Return a memoryview over the malloc’d array
    return <int8_t[:n]> result_buf

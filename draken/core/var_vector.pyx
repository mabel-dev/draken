# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdlib cimport calloc
from libc.stdlib cimport free
from libc.stdlib cimport malloc

from libc.stdint cimport int32_t
from libc.stdint cimport intptr_t
from libc.stdint cimport uint8_t

from draken.core.buffers cimport DRAKEN_STRING
from draken.core.buffers cimport DrakenVarBuffer

cdef DrakenVarBuffer* alloc_var_column(size_t length, size_t data_capacity):
    cdef DrakenVarBuffer* col = <DrakenVarBuffer*> malloc(sizeof(DrakenVarBuffer))
    col.data = <uint8_t*> malloc(data_capacity)
    col.offsets = <int32_t*> calloc(length + 1, sizeof(int32_t))
    col.null_bitmap = NULL
    col.length = length
    return col

cpdef object py_alloc_var_column(size_t length, size_t data_capacity):
    cdef DrakenVarBuffer* col = alloc_var_column(length, data_capacity)
    return <intptr_t>col

cdef class VarVector:
    cdef DrakenVarBuffer* ptr

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

cpdef int STRING():
    return DRAKEN_STRING

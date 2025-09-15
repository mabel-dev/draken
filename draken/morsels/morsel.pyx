# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False
# cython: infer_types=True

from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.mem cimport PyMem_Free
from cpython.mem cimport PyMem_Malloc
from libc.string cimport strlen

from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenType
from draken.core.buffers cimport DrakenMorsel


cdef class Morsel:
    cdef DrakenMorsel* ptr
    cdef list _encoded_names
    cdef list _columns

    def __dealloc__(self):
        if self.ptr is not NULL:
            PyMem_Free(self.ptr.column_names)
            PyMem_Free(self.ptr.column_types)
            PyMem_Free(self.ptr.columns)
            PyMem_Free(self.ptr)

    @staticmethod
    def from_arrow(object table):
        cdef int i, n = table.num_columns
        cdef Morsel self = Morsel()
        cdef Vector vec
        cdef bytes encoded_name

        self._columns = [None] * n
        self._encoded_names = [None] * n
        self.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
        self.ptr.num_columns = n
        self.ptr.num_rows = table.num_rows
        self.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n)
        self.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n)
        self.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n)

        for i in range(n):
            col = table.column(i)
            vec = Vector.from_arrow(col)
            self._columns[i] = vec

            name = table.schema.field(i).name
            encoded_name = name.encode("utf-8")
            self._encoded_names[i] = encoded_name

            self.ptr.columns[i] = <void*>vec
            self.ptr.column_types[i] = vec.dtype
            self.ptr.column_names[i] = <const char*>encoded_name

        return self

    def column(self, bytes name):
        for i in range(self.ptr.num_columns):
            if self.ptr.column_names[i] == name:
                return <Vector>self.ptr.columns[i]
        raise KeyError(f"Column '{name}' not found")

    @property
    def shape(self):
        """Return (num_rows, num_columns) tuple."""
        return (self.ptr.num_rows, self.ptr.num_columns)

    @property
    def num_rows(self):
        """Return the number of rows."""
        return self.ptr.num_rows

    @property
    def num_columns(self):
        """Return the number of columns."""
        return self.ptr.num_columns

    @property
    def column_names(self):
        """Return the list of column names."""
        cdef list names = []
        cdef size_t i
        cdef const char* cstr
        for i in range(self.ptr.num_columns):
            cstr = self.ptr.column_names[i]
            names.append(<str> PyBytes_FromStringAndSize(cstr, strlen(cstr)))
        return names

    def __repr__(self):
        return f"<Morsel: {self.ptr.num_rows} rows x {self.ptr.num_columns} columns>"

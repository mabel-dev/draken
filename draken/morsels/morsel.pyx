# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False
# cython: infer_types=True

"""
Morsel: Batch data container for columnar processing in Draken.

This module provides the Morsel class which represents a batch of columnar data
similar to Arrow's RecordBatch but optimized for Draken's internal processing.
Morsels contain multiple Vector columns and provide efficient batch operations
for analytical workloads.

The module includes:
- Morsel class for managing collections of Vector columns
- DrakenTypeInt helper for debugging type information
- Integration with Draken's core buffer management system
"""

from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.mem cimport PyMem_Free
from cpython.mem cimport PyMem_Malloc
from libc.string cimport strlen, strcmp
from libc.stdint cimport int32_t

from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenType, DrakenMorsel

# Python helper: int subclass for DrakenType enum debugging
cdef class DrakenTypeInt(int):
    def __repr__(self):
        return f"{self._enum_name()}({int(self)})"

    def __str__(self):
        return self._enum_name()

    def _enum_name(self):
        mapping = {
            1: "DRAKEN_INT8",
            2: "DRAKEN_INT16",
            3: "DRAKEN_INT32",
            4: "DRAKEN_INT64",
            20: "DRAKEN_FLOAT32",
            21: "DRAKEN_FLOAT64",
            30: "DRAKEN_DATE32",
            40: "DRAKEN_TIMESTAMP64",
            50: "DRAKEN_BOOL",
            60: "DRAKEN_STRING",
            80: "DRAKEN_ARRAY",
            100: "DRAKEN_NON_NATIVE",
        }
        return mapping.get(int(self), f"UNKNOWN({int(self)})")

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
    def column_names(self) -> list:
        """Return the list of column names."""
        cdef list names = []
        cdef size_t i
        cdef const char* cstr
        for i in range(self.ptr.num_columns):
            cstr = self.ptr.column_names[i]
            names.append(<str> PyBytes_FromStringAndSize(cstr, strlen(cstr)))
        return names

    @property
    def column_types(self):
        """Return the list of column types"""
        cdef list types = []
        cdef size_t i
        for i in range(self.ptr.num_columns):
            types.append(DrakenTypeInt(self.ptr.column_types[i]))
        return types

    def __getitem__(self, Py_ssize_t i) -> tuple:
        out = []
        for c in self._columns:
            try:
                out.append(c[i])
            except Exception:
                out.append(None)
        return tuple(out)

    def __repr__(self):
        return f"<Morsel: {self.ptr.num_rows} rows x {self.ptr.num_columns} columns>"

    # High-performance C/Cython implementations
    def take(self, indices):
        """
        Take rows by indices with high-performance C/Cython implementation.

        Args:
            indices: List or array of row indices to select

        Returns:
            Morsel: New Morsel with selected rows
        """
        cdef int32_t[::1] indices_view
        cdef int i, n_indices, n_columns = self.ptr.num_columns
        cdef Morsel result = Morsel()
        cdef Vector src_vec, dst_vec
        cdef DrakenType vec_type

        # Convert indices to array without NumPy
        if not hasattr(indices, '__len__'):
            indices = [indices]

        # Handle PyArrow arrays by converting to list
        if hasattr(indices, 'to_pylist'):
            indices = indices.to_pylist()
        elif hasattr(indices, 'tolist'):  # Handle numpy arrays if passed in
            indices = indices.tolist()

        # Convert to C array
        n_indices = len(indices)
        cdef int32_t* indices_ptr = <int32_t*>PyMem_Malloc(n_indices * sizeof(int32_t))
        if indices_ptr == NULL:
            raise MemoryError()

        try:
            for i in range(n_indices):
                indices_ptr[i] = <int32_t>indices[i]

            # Create memoryview from C array
            indices_view = <int32_t[:n_indices]>indices_ptr

            # Initialize result morsel
            result._columns = [None] * n_columns
            result._encoded_names = [None] * n_columns
            result.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
            result.ptr.num_columns = n_columns
            result.ptr.num_rows = n_indices
            result.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n_columns)
            result.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n_columns)
            result.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n_columns)

            # Take from each column using vector's native take method
            for i in range(n_columns):
                src_vec = <Vector>self.ptr.columns[i]
                vec_type = self.ptr.column_types[i]

                # All vector types should now have take method
                dst_vec = src_vec.take(indices_view)

                result._columns[i] = dst_vec
                result._encoded_names[i] = self._encoded_names[i]
                result.ptr.columns[i] = <void*>dst_vec
                result.ptr.column_types[i] = vec_type
                result.ptr.column_names[i] = self.ptr.column_names[i]

            return result
        finally:
            PyMem_Free(indices_ptr)

    def select(self, columns):
        """
        Select columns by name with high-performance C/Cython implementation.

        Args:
            columns: List of column names to select, or single column name

        Returns:
            Morsel: New Morsel with selected columns
        """
        cdef int i, j, n_selected
        cdef list column_indices = []
        cdef bytes col_name
        cdef Morsel result = Morsel()
        cdef Vector vec

        # Normalize columns to list
        if isinstance(columns, str):
            columns = [columns]
        elif isinstance(columns, bytes):
            columns = [columns]

        # Find column indices efficiently
        for col in columns:
            if isinstance(col, str):
                col_name = col.encode('utf-8')
            else:
                col_name = col

            for i in range(self.ptr.num_columns):
                if strcmp(self.ptr.column_names[i], col_name) == 0:
                    column_indices.append(i)
                    break
            else:
                raise KeyError(f"Column '{col}' not found")

        n_selected = len(column_indices)

        # Initialize result morsel
        result._columns = [None] * n_selected
        result._encoded_names = [None] * n_selected
        result.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
        result.ptr.num_columns = n_selected
        result.ptr.num_rows = self.ptr.num_rows
        result.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n_selected)
        result.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n_selected)
        result.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n_selected)

        # Copy selected columns
        for j, i in enumerate(column_indices):
            vec = <Vector>self.ptr.columns[i]
            result._columns[j] = vec
            result._encoded_names[j] = self._encoded_names[i]
            result.ptr.columns[j] = <void*>vec
            result.ptr.column_types[j] = self.ptr.column_types[i]
            result.ptr.column_names[j] = self.ptr.column_names[i]

        return result

    def rename(self, names):
        """
        Rename columns with high-performance C/Cython implementation.

        Args:
            names: List of new column names or dict mapping old->new names

        Returns:
            Morsel: New Morsel with renamed columns
        """
        cdef int i, n_columns = self.ptr.num_columns
        cdef list new_names = []
        cdef bytes encoded_name
        cdef Morsel result = Morsel()
        cdef Vector vec

        # Handle different name formats
        if isinstance(names, dict):
            # Dict mapping old->new names
            for i in range(n_columns):
                old_name = self.ptr.column_names[i].decode('utf-8')
                new_names.append(names.get(old_name, old_name))
        else:
            # List of new names
            if len(names) != n_columns:
                raise ValueError(f"Expected {n_columns} names, got {len(names)}")
            new_names = list(names)

        # Initialize result morsel
        result._columns = [None] * n_columns
        result._encoded_names = [None] * n_columns
        result.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
        result.ptr.num_columns = n_columns
        result.ptr.num_rows = self.ptr.num_rows
        result.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n_columns)
        result.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n_columns)
        result.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n_columns)

        # Copy columns with new names
        for i in range(n_columns):
            vec = <Vector>self.ptr.columns[i]
            encoded_name = new_names[i].encode('utf-8')

            result._columns[i] = vec
            result._encoded_names[i] = encoded_name
            result.ptr.columns[i] = <void*>vec
            result.ptr.column_types[i] = self.ptr.column_types[i]
            result.ptr.column_names[i] = <const char*>encoded_name

        return result

    def to_arrow(self):
        """
        Convert Morsel to Arrow Table with high-performance implementation.

        Returns:
            pyarrow.Table: Table with same data and column names
        """
        import pyarrow as pa

        # Get column names as strings
        column_names = []
        cdef int i
        for i in range(self.ptr.num_columns):
            column_names.append(self.ptr.column_names[i].decode('utf-8'))

        # Get arrow arrays from vectors using their native to_arrow methods
        arrow_columns = []
        cdef Vector vec
        for i in range(self.ptr.num_columns):
            vec = <Vector>self.ptr.columns[i]
            arrow_columns.append(vec.to_arrow())

        return pa.table(arrow_columns, names=column_names)

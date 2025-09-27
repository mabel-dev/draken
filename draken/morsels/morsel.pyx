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
from libc.string cimport strlen

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

    def __cinit__(self):
        self.ptr = NULL
        self._encoded_names = None
        self._columns = None

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

    def take(self, indices):
        """
        Take rows by indices, similar to pyarrow.Table.take.

        Args:
            indices: List or array of row indices to select

        Returns:
            Morsel: New Morsel with selected rows
        """
        import pyarrow as pa

        # First convert this morsel to arrow (bypassing potential vector issues)
        arrow_table = self._to_arrow_safe()

        # Use pyarrow's take method
        taken_table = arrow_table.take(indices)

        # Create new Morsel from the taken table
        return Morsel.from_arrow(taken_table)

    def select(self, columns):
        """
        Select columns by name, similar to pyarrow.Table.select.

        Args:
            columns: List of column names to select, or single column name string

        Returns:
            Morsel: New Morsel with selected columns
        """
        import pyarrow as pa

        # First convert this morsel to arrow (bypassing potential vector issues)
        arrow_table = self._to_arrow_safe()

        # Ensure columns is a list for pyarrow compatibility
        if isinstance(columns, str):
            columns = [columns]

        # Use pyarrow's select method
        selected_table = arrow_table.select(columns)

        # Create new Morsel from the selected table
        return Morsel.from_arrow(selected_table)

    def rename(self, names):
        """
        Rename columns, similar to pyarrow.Table.rename_columns.

        Args:
            names: List of new column names or dict mapping old->new names

        Returns:
            Morsel: New Morsel with renamed columns
        """
        import pyarrow as pa

        # First convert this morsel to arrow (bypassing potential vector issues)
        arrow_table = self._to_arrow_safe()

        if isinstance(names, dict):
            # Handle dict mapping by creating a list of names
            current_names = arrow_table.column_names
            new_names = [names.get(name, name) for name in current_names]
            renamed_table = arrow_table.rename_columns(new_names)
        else:
            # Handle list of names directly
            renamed_table = arrow_table.rename_columns(names)

        # Create new Morsel from the renamed table
        return Morsel.from_arrow(renamed_table)

    def to_arrow(self):
        """
        Convert Morsel back to pyarrow.Table.

        Returns:
            pyarrow.Table: Table with same data and column names
        """
        return self._to_arrow_safe()

    def _to_arrow_safe(self):
        """
        Reconstruct arrow table by building it column by column using row access.
        This is slower but guarantees correctness by avoiding the corrupted vector conversions.
        """
        import pyarrow as pa

        # Get column names as strings
        column_names = [name.decode('utf-8') if isinstance(name, bytes) else name
                        for name in self.column_names]

        # Build data row by row to avoid vector conversion issues
        columns_data = [[] for _ in range(self.ptr.num_columns)]

        # Extract data row by row
        cdef int row, col
        for row in range(self.ptr.num_rows):
            row_data = self[row]  # Get row as tuple
            for col in range(len(row_data)):
                columns_data[col].append(row_data[col])

        # Create pyarrow arrays from the extracted data
        arrow_columns = []
        for col_data in columns_data:
            try:
                arrow_columns.append(pa.array(col_data))
            except Exception as e:
                # If we can't create the array, try with explicit null handling
                processed_data = []
                for item in col_data:
                    if isinstance(item, bytes):
                        try:
                            # Try to decode bytes to string
                            processed_data.append(item.decode('utf-8'))
                        except UnicodeDecodeError:
                            processed_data.append(None)
                    else:
                        processed_data.append(item)
                arrow_columns.append(pa.array(processed_data))

        # Create and return the table
        return pa.table(arrow_columns, names=column_names)

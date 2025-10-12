from draken.vectors.vector cimport Vector

cdef class ArrayVector(Vector):
    cdef object _arr  # Store the arrow array

cdef ArrayVector array_from_arrow(object array)

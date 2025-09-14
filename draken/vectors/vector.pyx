# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from draken.interop.arrow cimport vector_from_arrow

cdef class Vector:

    @classmethod
    def from_arrow(cls, arrow_array):
        return vector_from_arrow(arrow_array)

    def __str__(self):
        return f"<{self.__class__.__name__} len={len(self)}>"

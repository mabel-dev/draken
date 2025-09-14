# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Arrow interoperability helpers for Draken columnar buffers.

This module provides:
- Functions to expose DrakenFixedBuffer as ArrowArray and ArrowSchema
- Memory management utilities for Arrow C Data Interface structs
- Conversion helpers for zero-copy Arrow integration

Used to enable efficient interchange between Draken and Apache Arrow for analytics and data science workflows.
"""

import pyarrow

from libc.stdlib cimport free
from libc.stdlib cimport malloc

from draken.core.buffers cimport DrakenFixedBuffer
from draken.interop.arrow_c_data_interface cimport ARROW_FLAG_NULLABLE
from draken.interop.arrow_c_data_interface cimport ArrowArray
from draken.interop.arrow_c_data_interface cimport ArrowSchema
from draken.vectors.arrow_vector import from_arrow as arrow_from_arrow
from draken.vectors.int64_vector cimport from_arrow as int64_from_arrow


cdef void release_arrow_array(ArrowArray* arr) noexcept:
    free(<void*>arr.buffers)
    free(arr)

cdef void release_arrow_schema(ArrowSchema* schema) noexcept:
    free(schema)

cdef void expose_draken_fixed_as_arrow(
    DrakenFixedBuffer* vec,
    ArrowArray** out_array,
    ArrowSchema** out_schema,
):
    cdef ArrowArray* arr = <ArrowArray*>malloc(sizeof(ArrowArray))
    cdef ArrowSchema* schema = <ArrowSchema*>malloc(sizeof(ArrowSchema))
    out_array[0] = arr
    out_schema[0] = schema

    # Fill ArrowArray
    arr.length = vec.length
    arr.null_count = -1
    arr.offset = 0
    arr.n_buffers = 2
    arr.n_children = 0
    arr.children = NULL
    arr.dictionary = NULL
    arr.release = release_arrow_array
    arr.private_data = NULL

    arr.buffers = <const void**>malloc(2 * sizeof(void*))
    arr.buffers[0] = <const void*>vec.null_bitmap
    arr.buffers[1] = vec.data

    # Fill ArrowSchema
    schema.format = b"l"
    schema.name = NULL
    schema.metadata = NULL
    schema.flags = ARROW_FLAG_NULLABLE if vec.null_bitmap != NULL else 0
    schema.n_children = 0
    schema.children = NULL
    schema.dictionary = NULL
    schema.release = release_arrow_schema
    schema.private_data = NULL


cpdef object vector_from_arrow(object array):

    if hasattr(array, "combine_chunks"):
        array = array.combine_chunks()

    pa_type = array.type
    if pa_type.equals(pyarrow.int64()):
        return int64_from_arrow(array)

    # fall back implementation (just wrap pyarrow compute)
    return arrow_from_arrow(array)

from libc.stdlib cimport malloc, free
from libc.stdint cimport int64_t, uintptr_t

from draken.src.draken_columns cimport DrakenFixedColumn
from draken.src.arrow_c_data_interface cimport ArrowArray
from draken.src.arrow_c_data_interface cimport ArrowSchema
from draken.src.arrow_c_data_interface cimport ARROW_FLAG_NULLABLE


cdef void release_arrow_array(ArrowArray* arr) noexcept:
    free(<void*>arr.buffers)
    free(arr)

cdef void release_arrow_schema(ArrowSchema* schema) noexcept:
    free(schema)

cdef void expose_draken_fixed_as_arrow(
    DrakenFixedColumn* col,
    ArrowArray** out_array,
    ArrowSchema** out_schema,
):
    cdef ArrowArray* arr = <ArrowArray*>malloc(sizeof(ArrowArray))
    cdef ArrowSchema* schema = <ArrowSchema*>malloc(sizeof(ArrowSchema))
    out_array[0] = arr
    out_schema[0] = schema

    # Fill ArrowArray
    arr.length = col.length
    arr.null_count = -1
    arr.offset = 0
    arr.n_buffers = 2
    arr.n_children = 0
    arr.children = NULL
    arr.dictionary = NULL
    arr.release = release_arrow_array
    arr.private_data = NULL

    arr.buffers = <const void**>malloc(2 * sizeof(void*))
    arr.buffers[0] = <const void*>col.null_bitmap
    arr.buffers[1] = col.data

    # Fill ArrowSchema
    schema.format = b"l"
    schema.name = NULL
    schema.metadata = NULL
    schema.flags = ARROW_FLAG_NULLABLE if col.null_bitmap != NULL else 0
    schema.n_children = 0
    schema.children = NULL
    schema.dictionary = NULL
    schema.release = release_arrow_schema
    schema.private_data = NULL
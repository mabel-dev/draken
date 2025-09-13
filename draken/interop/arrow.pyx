from libc.stdlib cimport malloc, free
from libc.stdint cimport int64_t, uintptr_t

from draken.core.buffers cimport DrakenFixedBuffer
from draken.interop.arrow_c_data_interface cimport ArrowArray
from draken.interop.arrow_c_data_interface cimport ArrowSchema
from draken.interop.arrow_c_data_interface cimport ARROW_FLAG_NULLABLE


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







import pyarrow
from libc.stdint cimport intptr_t, uint8_t

from draken.vectors.int64_vector cimport Int64Vector
from draken.core.buffers cimport DrakenFixedBuffer, DRAKEN_INT64

cpdef object vector_from_arrow(object array):
    pa_type = array.type
    if pa_type.equals(pyarrow.int64()):
        return _wrap_int64(array)
    # TODO: add float64, bool, string, etc.
    raise NotImplementedError(f"from_arrow: unsupported type {pa_type}")

cdef Int64Vector _wrap_int64(object array):
    cdef Int64Vector vec = Int64Vector(0, True)   # wrap=True: no alloc
    vec.ptr = <DrakenFixedBuffer*> malloc(sizeof(DrakenFixedBuffer))
    if vec.ptr == NULL:
        raise MemoryError()
    vec.owns_data = False

    cdef object bufs = array.buffers()
    cdef intptr_t base_ptr = <intptr_t> bufs[1].address
    cdef size_t itemsize = 8
    cdef Py_ssize_t offset = array.offset
    cdef intptr_t nb_addr

    vec.ptr.type = DRAKEN_INT64
    vec.ptr.itemsize = itemsize
    vec.ptr.length = <size_t> len(array)

    cdef intptr_t addr = base_ptr + offset * itemsize
    vec.ptr.data = <void*> addr

    if bufs[0] is not None:
        nb_addr = bufs[0].address
        vec.ptr.null_bitmap = <uint8_t*> nb_addr
    else:
        vec.ptr.null_bitmap = NULL

    return vec
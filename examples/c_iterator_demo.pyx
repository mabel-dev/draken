# cython: language_level=3
"""
Demonstration of C-level iterator for StringVector.

This example shows how to use the high-performance C iterator
for internal kernel operations.
"""

from libc.stdlib cimport malloc, realloc, free
from draken.vectors.string_vector cimport StringVector, _StringVectorCIterator, StringElement, StringVectorBuilder


cpdef void demonstrate_c_iterator():
    """Show usage of C-level iterator with nogil performance."""
    # Build a sample vector
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(5, 20)
    builder.append(b"hello")
    builder.append(b"world")
    builder.append_null()
    builder.append(b"foo")
    builder.append(b"bar")
    
    cdef StringVector vec = builder.finish()
    
    # Use C-level iterator
    cdef _StringVectorCIterator it = vec.c_iter()
    cdef StringElement elem
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t null_count = 0
    
    print("Iterating with C-level iterator:")
    # This loop can run with nogil for maximum performance
    while it.next(&elem):
        if elem.is_null:
            null_count += 1
            print(f"  Position {it.position - 1}: NULL")
        else:
            total_bytes += elem.length
            # elem.ptr and elem.length available for processing
            value = elem.ptr[:elem.length]  # Create bytes view
            print(f"  Position {it.position - 1}: {value} ({elem.length} bytes)")
    
    print(f"\nTotal non-null bytes: {total_bytes}")
    print(f"Null count: {null_count}")


cpdef void demonstrate_raw_pointer_builder():
    """Show usage of raw pointer append for zero-copy-friendly building."""
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(3, 30)
    
    # Simulate data coming from a C library or buffer
    cdef char* data1 = "first"
    cdef char* data2 = "second"
    cdef char* data3 = "third"
    
    # Append directly from raw pointers
    builder.append_bytes(data1, 5)
    builder.append_bytes(data2, 6)
    builder.append_bytes(data3, 5)
    
    cdef StringVector vec = builder.finish()
    
    print("\nVector built with raw pointer appends:")
    for i in range(len(vec)):
        print(f"  [{i}]: {vec[i]}")


cpdef Py_ssize_t compute_total_length(StringVector vec):
    """
    Example kernel: compute total byte length of all non-null strings.
    
    This demonstrates a high-performance operation using the C iterator
    that could run with nogil in a tight loop.
    """
    cdef _StringVectorCIterator it = vec.c_iter()
    cdef StringElement elem
    cdef Py_ssize_t total = 0
    
    while it.next(&elem):
        if not elem.is_null:
            total += elem.length
    
    return total


cpdef StringVector uppercase_kernel(StringVector vec):
    """
    Example kernel: uppercase all strings using C iterator.
    
    This shows how kernels can use the C iterator for input
    and raw pointer builder for output.
    """
    cdef _StringVectorCIterator it = vec.c_iter()
    cdef StringElement elem
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t i
    
    # First pass: calculate total bytes
    while it.next(&elem):
        if not elem.is_null:
            total_bytes += elem.length
    
    # Create builder
    cdef StringVectorBuilder builder = StringVectorBuilder(len(vec), total_bytes)
    
    # Reset iterator for second pass
    it.reset()
    
    # Allocate buffer for uppercase conversion
    cdef char* upper_buf = <char*>malloc(1024)  # Reusable buffer
    cdef Py_ssize_t buf_capacity = 1024
    cdef Py_ssize_t j
    cdef char ch
    
    try:
        while it.next(&elem):
            if elem.is_null:
                builder.append_null()
            else:
                # Ensure buffer is large enough
                if elem.length > buf_capacity:
                    buf_capacity = elem.length
                    upper_buf = <char*>realloc(upper_buf, buf_capacity)
                    if upper_buf == NULL:
                        raise MemoryError()
                
                # Convert to uppercase in reusable buffer
                for j in range(elem.length):
                    ch = elem.ptr[j]
                    if 97 <= ch <= 122:  # 'a'..'z'
                        upper_buf[j] = ch - 32
                    else:
                        upper_buf[j] = ch
                
                # Append from buffer using raw pointer method
                builder.append_bytes(upper_buf, elem.length)
        
        return builder.finish()
    finally:
        free(upper_buf)


# Python-visible demo function
def run_demos():
    """Run all C iterator demonstrations."""
    print("=" * 60)
    print("C-Level Iterator Demo")
    print("=" * 60)
    
    demonstrate_c_iterator()
    demonstrate_raw_pointer_builder()
    
    # Test compute_total_length
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(4, 20)
    builder.append(b"abc")
    builder.append(b"defgh")
    builder.append_null()
    builder.append(b"ij")
    cdef StringVector vec = builder.finish()
    
    total = compute_total_length(vec)
    print(f"\nTotal length computed: {total} (expected: 10)")
    
    # Test uppercase kernel
    print("\nUppercase kernel test:")
    cdef StringVector upper = uppercase_kernel(vec)
    for i in range(len(upper)):
        print(f"  [{i}]: {upper[i]}")

# StringVector C-Level API Implementation Summary

## Overview

This document summarizes the C-level API improvements made to `StringVector` to support high-performance internal kernel operations.

## Features Implemented

### 1. C-Level Iterator (`_StringVectorCIterator`)

A nogil-compatible iterator that yields lightweight `StringElement` structs for minimal-overhead string processing.

#### The StringElement Struct

```cython
# Lightweight struct for C-level iteration over string vector elements
cdef struct StringElement:
    char* ptr           # Pointer to string data (NULL if null)
    Py_ssize_t length   # Length in bytes
    bint is_null        # True if element is null
```

#### Usage from Cython

```cython
from draken.vectors.string_vector cimport StringVector, _StringVectorCIterator, StringElement

cdef StringVector vec = ...
cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem

# Iterate with nogil capability
while it.next(&elem):
    if not elem.is_null:
        # Process elem.ptr and elem.length
        process_string(elem.ptr, elem.length)
```

#### API Methods

- `next(StringElement* elem) -> bint` (nogil): Advance iterator, populate struct
- `reset()`: Reset to beginning
- `get_at(index) -> StringElement`: Random access without advancing
- `position`: Property returning current position

#### Key Benefits

- **No Python object creation**: Yields raw pointers and lengths
- **NoGIL compatible**: Can run in parallel/CPU-intensive loops
- **Minimal overhead**: Direct memory access without indirection
- **Reusable**: Reset for multiple passes over data

### 2. Raw Pointer Builder Methods

Zero-copy-friendly append methods that bypass Python bytes object creation.

#### New Methods on StringVectorBuilder

```cython
cpdef void append_bytes(self, const char* ptr, Py_ssize_t length)
cpdef void set_bytes(self, Py_ssize_t index, const char* ptr, Py_ssize_t length)
```

#### Usage from Cython

```cython
from draken.vectors.string_vector cimport StringVectorBuilder

cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, capacity)

# Append from raw C strings
cdef char* data = get_c_string()
builder.append_bytes(data, strlen(data))

# From buffer with known length
cdef char* buf = buffer + offset
builder.append_bytes(buf, length)
```

#### Key Benefits

- **Avoids Python bytes intermediaries**: Direct pointer copy
- **Integration with C libraries**: Can use data directly from C APIs
- **Memory efficient**: No temporary object allocation
- **Flexible**: Works with any `const char*` source

## Implementation Details

### Files Modified

1. **draken/vectors/string_vector.pyx**
   - Added `StringElement` struct definition
   - Implemented `_StringVectorCIterator` class
   - Added `c_iter()` method to `StringVector`
   - Added `append_bytes()` and `set_bytes()` to `StringVectorBuilder`

2. **draken/vectors/string_vector.pxd**
   - Exposed `StringElement` struct for Cython imports
   - Declared `_StringVectorCIterator` class with cdef members
   - Exposed `StringVectorBuilder` with new methods
   - Declared private helper methods

### Design Patterns

#### Two-Pass Pattern with Reset

```cython
# First pass: measure
cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem
cdef Py_ssize_t total_bytes = 0

while it.next(&elem):
    if not elem.is_null:
        total_bytes += elem.length

# Allocate based on measurement
cdef StringVectorBuilder builder = StringVectorBuilder(len(vec), total_bytes)

# Second pass: populate
it.reset()
while it.next(&elem):
    if not elem.is_null:
        # Process and append
        ...
```

#### Reusable Buffer Pattern

```cython
# Allocate one buffer for all operations
cdef char* buf = <char*>malloc(initial_size)
cdef Py_ssize_t buf_capacity = initial_size

cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem

while it.next(&elem):
    if not elem.is_null:
        # Resize if needed
        if elem.length > buf_capacity:
            buf_capacity = elem.length
            buf = <char*>realloc(buf, buf_capacity)
        
        # Process in buffer
        transform(elem.ptr, elem.length, buf)
        builder.append_bytes(buf, elem.length)

free(buf)
```

## Performance Characteristics

### C Iterator vs Python Iterator

| Feature | Python Iterator | C Iterator |
|---------|----------------|------------|
| Object creation | Yes (bytes per element) | No |
| GIL required | Yes | No |
| Overhead | High | Minimal |
| Use case | Python code | Cython kernels |

### Memory Access Patterns

- **Sequential scans**: Use C iterator (`O(1)` per element)
- **Random access**: Use View API (`O(1)` per lookup)
- **Python iteration**: Use standard iterator (convenience)

## Example Use Cases

### 1. String Length Computation

```cython
cpdef Py_ssize_t total_length(StringVector vec):
    """Compute total byte length of all non-null strings."""
    cdef _StringVectorCIterator it = vec.c_iter()
    cdef StringElement elem
    cdef Py_ssize_t total = 0
    
    while it.next(&elem):
        if not elem.is_null:
            total += elem.length
    
    return total
```

### 2. Uppercase Transformation

```cython
cpdef StringVector uppercase(StringVector vec):
    """Convert all strings to uppercase with reusable buffer."""
    cdef _StringVectorCIterator it = vec.c_iter()
    cdef StringElement elem
    cdef Py_ssize_t total = 0
    
    # Measure
    while it.next(&elem):
        if not elem.is_null:
            total += elem.length
    
    # Build
    cdef StringVectorBuilder builder = StringVectorBuilder(len(vec), total)
    cdef char* buf = <char*>malloc(1024)
    cdef Py_ssize_t cap = 1024
    
    it.reset()
    while it.next(&elem):
        if elem.is_null:
            builder.append_null()
        else:
            if elem.length > cap:
                cap = elem.length
                buf = <char*>realloc(buf, cap)
            
            # Transform in buffer
            for i in range(elem.length):
                c = elem.ptr[i]
                buf[i] = c - 32 if 97 <= c <= 122 else c
            
            builder.append_bytes(buf, elem.length)
    
    free(buf)
    return builder.finish()
```

### 3. Integration with C Libraries

```cython
cdef StringVector parse_csv(FILE* fp, Py_ssize_t n_rows):
    """Parse CSV column using C FILE I/O."""
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(
        n_rows, n_rows * 32
    )
    cdef char line[4096]
    cdef Py_ssize_t length
    
    for _ in range(n_rows):
        if fgets(line, 4096, fp) != NULL:
            length = strlen(line)
            if length > 0 and line[length-1] == '\n':
                length -= 1
            builder.append_bytes(line, length)
        else:
            builder.append_null()
    
    return builder.finish()
```

## Testing

### Python Tests

Run basic functionality tests:
```bash
PYTHONPATH=. python tests/test_string_vector_c_api.py
```

### Cython Examples

For full C-level testing with struct access, compile and run:
```bash
# examples/c_iterator_demo.pyx contains comprehensive examples
# (Would need to be added to setup.py for compilation)
```

## Application to Other Vectors

These patterns can be extended to other vector types:

### Int64Vector C Iterator

```cython
cdef struct Int64Element:
    int64_t value
    bint is_null

cdef class _Int64VectorCIterator:
    cdef bint next(self, Int64Element* elem) nogil:
        # Similar pattern
        ...
```

### Float64Vector C Iterator

```cython
cdef struct Float64Element:
    double value
    bint is_null

cdef class _Float64VectorCIterator:
    cdef bint next(self, Float64Element* elem) nogil:
        # Similar pattern
        ...
```

## Backward Compatibility

All changes are additions only:

- Existing `StringVector` API unchanged
- Existing `StringVectorBuilder` methods unchanged
- New methods are opt-in for high-performance use cases
- Python code continues to work without modification

## Future Enhancements

Potential improvements:

1. **Batch iteration**: `next_batch(elem[], n)` for SIMD-friendly processing
2. **Filtered iteration**: Skip nulls automatically for certain operations
3. **Parallel iteration**: Split iterator for multi-threading
4. **Zero-copy slicing**: Create sub-iterators without data copy

## Documentation

- **API Reference**: `/docs/STRING_VECTOR_C_API.md`
- **Examples**: `/examples/string_vector_c_api_demo.py`
- **Cython Examples**: `/examples/c_iterator_demo.pyx`
- **Tests**: `/tests/test_string_vector_c_api.py`

## Summary

The C-level API additions provide:

✅ **Performance**: NoGIL iteration without Python object overhead  
✅ **Flexibility**: Direct pointer access for C library integration  
✅ **Ergonomics**: Clean API that follows existing patterns  
✅ **Compatibility**: Non-breaking additions to existing codebase  
✅ **Extensibility**: Pattern applicable to all vector types

These improvements enable high-performance kernel operations while maintaining the clean, Pythonic API for general use.

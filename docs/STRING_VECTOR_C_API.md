# StringVector C-Level API Improvements

This document describes the C-level API improvements to `StringVector` that provide high-performance access for internal kernel operations.

## Overview

Two main improvements have been added to `StringVector`:

1. **C-Level Iterator** - A nogil-compatible iterator that yields lightweight structs instead of Python objects
2. **Raw Pointer Builder Methods** - Zero-copy-friendly append methods that avoid Python bytes intermediaries

## 1. C-Level Iterator

### The `StringElement` Struct

A lightweight C struct representing a string element:

```cython
cdef struct StringElement:
    char* ptr        # Pointer to the string data (NULL if null)
    Py_ssize_t length  # Length of the string in bytes
    bint is_null     # True if this element is null
```

### Usage

#### Basic Iteration

```cython
from draken.vectors.string_vector cimport StringVector, _StringVectorCIterator, StringElement

cdef StringVector vec = get_vector()
cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem

# Iterate over all elements
while it.next(&elem):
    if not elem.is_null:
        # Use elem.ptr and elem.length
        process_string(elem.ptr, elem.length)
    else:
        # Handle null
        pass
```

#### NoGIL Performance

The iterator's `next()` method is `nogil` compatible for maximum performance:

```cython
cdef Py_ssize_t compute_total_length(StringVector vec) nogil:
    cdef _StringVectorCIterator it = vec.c_iter()
    cdef StringElement elem
    cdef Py_ssize_t total = 0
    
    while it.next(&elem):
        if not elem.is_null:
            total += elem.length
    
    return total
```

#### Iterator Methods

- `next(StringElement* elem) -> bint`: Advance and populate elem. Returns False when done.
- `reset()`: Reset iterator to beginning for another pass
- `get_at(index) -> StringElement`: Random access without advancing position
- `position`: Property returning current iterator position

### Example: String Length Kernel

```cython
cpdef Py_ssize_t[::1] string_lengths(StringVector vec):
    """Compute lengths of all strings in vector."""
    cdef Py_ssize_t n = len(vec)
    cdef Py_ssize_t[::1] result = np.empty(n, dtype=np.int64)
    cdef _StringVectorCIterator it = vec.c_iter()
    cdef StringElement elem
    cdef Py_ssize_t i = 0
    
    while it.next(&elem):
        if elem.is_null:
            result[i] = -1  # or handle nulls differently
        else:
            result[i] = elem.length
        i += 1
    
    return result
```

### Example: Uppercase Kernel with Reusable Buffer

```cython
cpdef StringVector uppercase_optimized(StringVector vec):
    """Convert all strings to uppercase using a reusable buffer."""
    cdef _StringVectorCIterator it = vec.c_iter()
    cdef StringElement elem
    cdef Py_ssize_t total_bytes = 0
    
    # First pass: calculate total bytes
    while it.next(&elem):
        if not elem.is_null:
            total_bytes += elem.length
    
    # Create output builder
    cdef StringVectorBuilder builder = StringVectorBuilder(len(vec), total_bytes)
    
    # Reset for second pass
    it.reset()
    
    # Allocate reusable buffer (avoids per-string allocation)
    cdef char* buf = <char*>malloc(1024)
    cdef Py_ssize_t buf_cap = 1024
    cdef Py_ssize_t i
    cdef char ch
    
    try:
        while it.next(&elem):
            if elem.is_null:
                builder.append_null()
            else:
                # Resize buffer if needed
                if elem.length > buf_cap:
                    buf_cap = elem.length
                    buf = <char*>realloc(buf, buf_cap)
                
                # Convert to uppercase in buffer
                for i in range(elem.length):
                    ch = elem.ptr[i]
                    buf[i] = ch - 32 if 97 <= ch <= 122 else ch
                
                # Append from buffer
                builder.append_bytes(buf, elem.length)
        
        return builder.finish()
    finally:
        free(buf)
```

## 2. Raw Pointer Builder Methods

### Overview

New methods on `StringVectorBuilder` that accept raw `const char*` pointers and lengths, avoiding the need to create Python bytes objects.

### API

```cython
cdef class StringVectorBuilder:
    # Existing methods
    cpdef void append(self, bytes value)
    cpdef void set(self, Py_ssize_t index, bytes value)
    
    # New methods
    cpdef void append_bytes(self, const char* ptr, Py_ssize_t length)
    cpdef void set_bytes(self, Py_ssize_t index, const char* ptr, Py_ssize_t length)
```

### Usage Examples

#### Appending from C Strings

```cython
cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(3, 30)

# Direct from C string literals
cdef char* s1 = "hello"
cdef char* s2 = "world"

builder.append_bytes(s1, 5)
builder.append_bytes(s2, 5)

cdef StringVector vec = builder.finish()
```

#### Building from External Buffer

```cython
cdef StringVector from_buffer(const char* data, Py_ssize_t* offsets, Py_ssize_t n):
    """Build StringVector from pre-existing buffer with offsets."""
    cdef Py_ssize_t total_bytes = offsets[n]
    cdef StringVectorBuilder builder = StringVectorBuilder(n, total_bytes)
    
    cdef Py_ssize_t i
    for i in range(n):
        builder.append_bytes(
            data + offsets[i],
            offsets[i+1] - offsets[i]
        )
    
    return builder.finish()
```

#### Integration with C Libraries

```cython
cdef StringVector parse_csv_column(FILE* fp, Py_ssize_t n_rows):
    """Parse CSV column from C FILE pointer."""
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n_rows, n_rows * 20)
    cdef char* line = <char*>malloc(4096)
    cdef Py_ssize_t length
    
    try:
        for _ in range(n_rows):
            if fgets(line, 4096, fp) == NULL:
                builder.append_null()
            else:
                # Strip newline
                length = strlen(line)
                if length > 0 and line[length-1] == '\n':
                    length -= 1
                builder.append_bytes(line, length)
        
        return builder.finish()
    finally:
        free(line)
```

## Performance Considerations

### Reusable Buffers

When processing strings, reuse a single buffer instead of allocating per element:

```cython
# Good: Reuse buffer
cdef char* buf = <char*>malloc(initial_size)
for ...:
    # resize buf if needed
    process_into(buf, ...)
    builder.append_bytes(buf, length)
free(buf)

# Bad: Allocate per element
for ...:
    cdef char* buf = <char*>malloc(length)
    process_into(buf, ...)
    builder.append_bytes(buf, length)
    free(buf)  # Repeated allocation overhead
```

### Two-Pass Patterns

For operations that need to know total size, use iterator's `reset()`:

```cython
# First pass: measure
cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem
cdef Py_ssize_t total = 0
while it.next(&elem):
    total += compute_output_size(elem)

# Allocate based on measurement
cdef StringVectorBuilder builder = StringVectorBuilder(len(vec), total)

# Second pass: populate
it.reset()
while it.next(&elem):
    process_and_append(elem, builder)
```

### NoGIL Opportunities

The C iterator can be used in nogil contexts for CPU-intensive operations:

```cython
cdef void process_large_vector(StringVector vec) nogil:
    cdef _StringVectorCIterator it = vec.c_iter()
    cdef StringElement elem
    
    while it.next(&elem):
        if not elem.is_null:
            # CPU-intensive processing without GIL
            hash_string(elem.ptr, elem.length)
```

## Comparison with Existing APIs

### Python Iterator (Returns bytes objects)

```python
# Creates Python bytes objects - higher overhead
for value in vec:
    if value is not None:
        process(value)
```

### View API (Random access)

```cython
# Good for random access, not sequential scans
cdef _StringVectorView view = vec.view()
for i in range(len(vec)):
    if not view.is_null(i):
        ptr = view.value_ptr(i)
        length = view.value_len(i)
```

### C Iterator (Sequential scans)

```cython
# Optimal for sequential processing
cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem
while it.next(&elem):
    if not elem.is_null:
        process(elem.ptr, elem.length)
```

## When to Use Each API

| Use Case | Recommended API |
|----------|----------------|
| Python code | Standard Python iteration |
| Random access from Cython | `vec.view()` |
| Sequential scan from Cython | `vec.c_iter()` |
| NoGIL processing | `vec.c_iter()` |
| Building from C strings | `builder.append_bytes()` |
| Building from Python bytes | `builder.append()` |
| Building from memoryview | `builder.append_view()` |

## Application to Other Vectors

These patterns can be applied to other vector types (Int64Vector, Float64Vector, etc.):

1. **Fixed-width vectors**: Struct could be simpler (just pointer + is_null)
2. **Numeric vectors**: Iterator yields primitive types directly
3. **Array vectors**: Nested iteration over child elements

Example for Int64Vector:

```cython
cdef struct Int64Element:
    int64_t value
    bint is_null

cdef class _Int64VectorCIterator:
    cdef bint next(self, Int64Element* elem) nogil:
        # Similar pattern to StringElement
        ...
```

## Summary

These improvements provide:

- **Zero Python overhead** for internal kernels via C iterator
- **NoGIL compatibility** for parallel/CPU-intensive operations
- **Memory efficiency** through reusable buffers
- **Clean API** that integrates with existing StringVector design

The patterns demonstrated here establish a foundation for similar optimizations across all vector types in Draken.

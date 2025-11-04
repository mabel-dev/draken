# StringVector C API Quick Reference

## When to Use What

| Task | Use This | Example |
|------|----------|---------|
| Python iteration | Standard `for` loop | `for val in vec: ...` |
| Cython sequential scan | C iterator | `while it.next(&elem): ...` |
| Random access (Cython) | View API | `view.value_ptr(i)` |
| Build from Python bytes | `append()` | `builder.append(b"text")` |
| Build from C strings | `append_bytes()` | `builder.append_bytes(ptr, len)` |
| Build from memoryview | `append_view()` | `builder.append_view(view)` |

## C Iterator Quick Start

### Import
```cython
from draken.vectors.string_vector cimport (
    StringVector, 
    _StringVectorCIterator, 
    StringElement,
    StringVectorBuilder
)
```

### Basic Loop
```cython
cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem

while it.next(&elem):
    if not elem.is_null:
        # elem.ptr is char*
        # elem.length is Py_ssize_t
        process(elem.ptr, elem.length)
```

### With Reset (Two-Pass)
```cython
cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem

# First pass
while it.next(&elem):
    measure(elem)

# Second pass
it.reset()
while it.next(&elem):
    process(elem)
```

### NoGIL Loop
```cython
cdef void kernel(StringVector vec) nogil:
    cdef _StringVectorCIterator it = vec.c_iter()
    cdef StringElement elem
    
    while it.next(&elem):
        if not elem.is_null:
            # Process without GIL
            hash_string(elem.ptr, elem.length)
```

## Builder Quick Start

### From Python
```python
from draken.vectors.string_vector import StringVectorBuilder

builder = StringVectorBuilder.with_estimate(n_rows, avg_bytes * n_rows)
builder.append(b"text")
builder.append_null()
vec = builder.finish()
```

### From Cython (bytes)
```cython
cdef StringVectorBuilder builder = StringVectorBuilder.with_counts(n, total_bytes)
builder.append(b"hello")
builder.append(<bytes>py_object)
```

### From Cython (raw pointers)
```cython
cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, capacity)

cdef char* data = get_data()
cdef Py_ssize_t length = get_length()

builder.append_bytes(data, length)
```

### From Cython (buffer)
```cython
cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, capacity)
cdef char buffer[1024]

# Fill buffer...
builder.append_bytes(buffer, actual_length)
```

## Common Patterns

### Pattern: Measure-Then-Build
```cython
# Pass 1: Measure
cdef Py_ssize_t total = 0
cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem

while it.next(&elem):
    total += compute_output_size(elem)

# Pass 2: Build
cdef StringVectorBuilder builder = StringVectorBuilder(len(vec), total)
it.reset()

while it.next(&elem):
    # Process and append
    builder.append_bytes(result_ptr, result_len)

return builder.finish()
```

### Pattern: Reusable Buffer
```cython
# Allocate once
cdef char* buf = <char*>malloc(1024)
cdef Py_ssize_t cap = 1024

cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem
cdef StringVectorBuilder builder = ...

while it.next(&elem):
    # Resize if needed
    if elem.length > cap:
        cap = elem.length
        buf = <char*>realloc(buf, cap)
    
    # Transform in buffer
    transform(elem.ptr, elem.length, buf)
    
    # Append from buffer
    builder.append_bytes(buf, transformed_length)

free(buf)
```

### Pattern: Filter Non-Nulls
```cython
cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem
cdef list results = []

while it.next(&elem):
    if not elem.is_null:
        # Process only non-null values
        results.append(process(elem.ptr, elem.length))
```

### Pattern: Copy with Transformation
```cython
cpdef StringVector transform(StringVector vec):
    cdef _StringVectorCIterator it = vec.c_iter()
    cdef StringElement elem
    cdef Py_ssize_t total = 0
    
    while it.next(&elem):
        if not elem.is_null:
            total += elem.length
    
    cdef StringVectorBuilder builder = StringVectorBuilder(len(vec), total)
    it.reset()
    
    while it.next(&elem):
        if elem.is_null:
            builder.append_null()
        else:
            # Transform and append
            transformed = transform_string(elem.ptr, elem.length)
            builder.append_bytes(transformed, ...)
    
    return builder.finish()
```

## Performance Tips

1. **Use C iterator for sequential scans** - Avoid repeated `vec[i]` in Cython
2. **Reuse buffers** - Don't malloc/free in tight loops
3. **Two-pass when size unknown** - Measure first, then allocate exact size
4. **Use nogil when possible** - Release GIL for CPU-intensive work
5. **Batch allocations** - Allocate large buffer, use incrementally

## Common Mistakes

❌ **Don't**: Create Python bytes in tight loop
```cython
for i in range(len(vec)):
    val = vec[i]  # Creates Python bytes object
    process(val)
```

✅ **Do**: Use C iterator
```cython
cdef _StringVectorCIterator it = vec.c_iter()
cdef StringElement elem
while it.next(&elem):
    if not elem.is_null:
        process(elem.ptr, elem.length)
```

❌ **Don't**: Malloc in loop
```cython
while it.next(&elem):
    cdef char* buf = <char*>malloc(elem.length)
    transform(elem.ptr, buf, elem.length)
    free(buf)  # Repeated allocation
```

✅ **Do**: Reuse buffer
```cython
cdef char* buf = <char*>malloc(max_size)
while it.next(&elem):
    transform(elem.ptr, buf, elem.length)
free(buf)
```

## StringElement Struct Reference

```cython
cdef struct StringElement:
    char* ptr         # Pointer to data (NULL if is_null)
    Py_ssize_t length # Byte length (0 if null or empty)
    bint is_null      # True if NULL, False otherwise
```

### Accessing Data
```cython
cdef StringElement elem
# After it.next(&elem):

if elem.is_null:
    # Handle NULL
    pass
else:
    # elem.ptr[0] through elem.ptr[elem.length-1] are valid
    for i in range(elem.length):
        char c = elem.ptr[i]
        # Process c
```

### Creating Bytes View (when needed)
```cython
cdef StringElement elem
# After it.next(&elem):

if not elem.is_null:
    # Create temporary bytes object for Python API
    py_bytes = elem.ptr[:elem.length]
    # or
    py_bytes = PyBytes_FromStringAndSize(elem.ptr, elem.length)
```

## Builder API Reference

### Constructor Methods
```cython
# Exact capacity (strict)
builder = StringVectorBuilder.with_counts(n_rows, total_bytes)

# Estimated capacity (resizable)
builder = StringVectorBuilder.with_estimate(n_rows, avg_bytes)

# Direct construction
builder = StringVectorBuilder(n_rows, capacity, resizable=True)
```

### Append Methods
```cython
builder.append(b"bytes")                    # Python bytes
builder.append_bytes(ptr, length)            # C pointer
builder.append_view(memoryview)             # Memoryview
builder.append_null()                        # NULL value
```

### Set Methods (positional)
```cython
builder.set(index, b"bytes")                # Python bytes
builder.set_bytes(index, ptr, length)       # C pointer
builder.set_view(index, memoryview)         # Memoryview
builder.set_null(index)                     # NULL value
```

### Properties
```cython
builder.bytes_capacity    # Total capacity
builder.bytes_used       # Currently used
builder.remaining_bytes  # Capacity - used
```

### Finish
```cython
vec = builder.finish()  # Returns StringVector, builder becomes unusable
```

## See Also

- Full documentation: `/docs/STRING_VECTOR_C_API.md`
- Implementation summary: `/docs/STRING_VECTOR_C_API_SUMMARY.md`
- Examples: `/examples/string_vector_c_api_demo.py`
- Cython examples: `/examples/c_iterator_demo.pyx`

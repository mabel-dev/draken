# Operation Dispatch System

## Overview

The operation dispatch system provides a type-safe way to dispatch binary operations based on operand types and scalarity. It's implemented in C++ for performance with a Cython wrapper for Python access.

## Architecture

The dispatch system consists of:

1. **C++ Header (`ops.h`)**: Defines operation enums and function pointer types
2. **C++ Implementation (`ops_impl.cpp`)**: Implements the `get_op` function
3. **Cython Definition (`ops.pxd`)**: Exposes C++ types to Cython
4. **Cython Wrapper (`ops.pyx`)**: Provides Python-friendly interface

## Main API

The primary function for operation dispatch is:

```python
get_op(left_type, left_is_scalar, right_type, right_is_scalar, operation) -> function or None
```

**Parameters:**
- `left_type`: Type of the left operand (use TYPE_* constants)
- `left_is_scalar`: Whether the left operand is a scalar (bool)
- `right_type`: Type of the right operand (use TYPE_* constants)
- `right_is_scalar`: Whether the right operand is a scalar (bool)
- `operation`: Operation to perform (string name or enum value)

**Returns:**
- `None` if the operation is not supported for the given type combination
- Function pointer (as integer) if the operation is supported

This matches the exact signature requested: `get_op(left_type, left_is_scalar, right_type, right_is_scalar, operation) -> function`

## Usage

### Getting Operation Enums

```python
from draken.core.ops import get_operation_enum

# Arithmetic operations
add_op = get_operation_enum('add')           # 1
subtract_op = get_operation_enum('subtract') # 2
multiply_op = get_operation_enum('multiply') # 3
divide_op = get_operation_enum('divide')     # 4

# Comparison operations
equals_op = get_operation_enum('equals')                        # 10
not_equals_op = get_operation_enum('not_equals')                # 11
greater_than_op = get_operation_enum('greater_than')            # 12
greater_than_or_equals_op = get_operation_enum('greater_than_or_equals')  # 13
less_than_op = get_operation_enum('less_than')                  # 14
less_than_or_equals_op = get_operation_enum('less_than_or_equals')  # 15

# Boolean operations
and_op = get_operation_enum('and')  # 20
or_op = get_operation_enum('or')    # 21
xor_op = get_operation_enum('xor')  # 22
```

### Using Type Constants

```python
from draken.core.ops import (
    TYPE_INT8, TYPE_INT16, TYPE_INT32, TYPE_INT64,
    TYPE_FLOAT32, TYPE_FLOAT64,
    TYPE_DATE32, TYPE_TIMESTAMP64, TYPE_TIME32, TYPE_TIME64,
    TYPE_BOOL, TYPE_STRING, TYPE_ARRAY, TYPE_NON_NATIVE
)
```

### Dispatching Operations

```python
from draken.core.ops import get_op, TYPE_INT64, TYPE_FLOAT64

# Method 1: Using the get_op function (simplest)
# Accepts operation as string or enum
result = get_op(
    left_type=TYPE_INT64,
    left_is_scalar=False,     # Left operand is a vector
    right_type=TYPE_INT64,
    right_is_scalar=True,      # Right operand is a scalar
    operation='equals'         # String or enum
)

if result is None:
    print("Operation not supported for these types")
else:
    print(f"Operation function pointer: {result}")

# Method 2: Using dispatch_op with explicit enum
from draken.core.ops import dispatch_op, get_operation_enum

equals_op = get_operation_enum('equals')
result = dispatch_op(
    left_type=TYPE_INT64,
    left_is_scalar=False,
    right_type=TYPE_INT64,
    right_is_scalar=True,
    operation=equals_op
)
```

## Operation Compatibility

### Comparison Operations

Comparison operations (`equals`, `not_equals`, `greater_than`, `greater_than_or_equals`, `less_than`, `less_than_or_equals`) require:
- Both operands to have the **same type**

Example:
```python
# ✓ Supported: int64 == int64
dispatch_op(TYPE_INT64, False, TYPE_INT64, True, equals_op)

# ✗ Not supported: int64 == float64 (different types)
dispatch_op(TYPE_INT64, False, TYPE_FLOAT64, True, equals_op)
```

### Arithmetic Operations

Arithmetic operations (`add`, `subtract`, `multiply`, `divide`) require:
- Both operands to be **numeric types** (int8-int64 or float32-float64)
- Both operands to have the **same type**

Example:
```python
add_op = get_operation_enum('add')

# ✓ Supported: int64 + int64
dispatch_op(TYPE_INT64, False, TYPE_INT64, False, add_op)

# ✓ Supported: float64 + float64
dispatch_op(TYPE_FLOAT64, False, TYPE_FLOAT64, True, add_op)

# ✗ Not supported: int32 + int64 (different types)
dispatch_op(TYPE_INT32, False, TYPE_INT64, False, add_op)
```

### Boolean Operations

Boolean operations (`and`, `or`, `xor`) require:
- Both operands to be **boolean type**

Example:
```python
and_op = get_operation_enum('and')

# ✓ Supported: bool AND bool
dispatch_op(TYPE_BOOL, False, TYPE_BOOL, False, and_op)

# ✗ Not supported: int64 AND int64
dispatch_op(TYPE_INT64, False, TYPE_INT64, False, and_op)
```

## Scalarity

The dispatch system supports the following combinations of scalar/vector operands:

- **Vector-Vector**: Both operands are vectors
- **Vector-Scalar**: Left is vector, right is scalar
- **Scalar-Scalar**: Both operands are scalars

**Note**: Scalar-Vector (left is scalar, right is vector) is **NOT supported** and will return `None`.

```python
# Vector-Vector
dispatch_op(TYPE_INT64, False, TYPE_INT64, False, add_op)

# Vector-Scalar
dispatch_op(TYPE_INT64, False, TYPE_INT64, True, add_op)

# Scalar-Vector (NOT SUPPORTED - returns None)
dispatch_op(TYPE_INT64, True, TYPE_INT64, False, add_op)  # Returns None

# Scalar-Scalar
dispatch_op(TYPE_INT64, True, TYPE_INT64, True, add_op)
```

## Return Values

The `dispatch_op` function returns:

- **None**: If the operation is not supported for the given type combination
- **Integer (function pointer)**: If the operation is supported (currently always returns None as operations are implemented in vector classes)

## Implementation Notes

1. The C++ implementation checks type compatibility but currently returns `NULL` for all cases
2. The actual operation implementations are in the type-specific vector classes (Int64Vector, Float64Vector, etc.)
3. This system serves as a type checker and compatibility validator
4. Future enhancements could return actual C++ function pointers for direct operation execution

## Adding New Operations

To add a new operation:

1. Add the operation to the `DrakenOperation` enum in `ops.h`
2. Update the `types_compatible` function in `ops_impl.cpp` to handle the new operation
3. Add the operation name to the `op_map` in `ops.pyx`
4. Add tests for the new operation in `tests/test_ops_dispatch.py`

## Type Enums

The following type enums are available:

| Type | Enum Value | Constant |
|------|------------|----------|
| INT8 | 1 | TYPE_INT8 |
| INT16 | 2 | TYPE_INT16 |
| INT32 | 3 | TYPE_INT32 |
| INT64 | 4 | TYPE_INT64 |
| FLOAT32 | 20 | TYPE_FLOAT32 |
| FLOAT64 | 21 | TYPE_FLOAT64 |
| DATE32 | 30 | TYPE_DATE32 |
| TIMESTAMP64 | 40 | TYPE_TIMESTAMP64 |
| TIME32 | 41 | TYPE_TIME32 |
| TIME64 | 42 | TYPE_TIME64 |
| BOOL | 50 | TYPE_BOOL |
| STRING | 60 | TYPE_STRING |
| ARRAY | 80 | TYPE_ARRAY |
| NON_NATIVE | 100 | TYPE_NON_NATIVE |

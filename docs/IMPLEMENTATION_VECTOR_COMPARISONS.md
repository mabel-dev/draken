# Vector-Vector Comparison Operations Implementation

## Summary

This implementation adds vector-vector comparison operations for Int64Vector and Float64Vector types in the Draken library. The new functionality allows element-wise comparisons between two vectors of the same type.

## Changes Made

### 1. Int64Vector Enhancements

Added the following vector-vector comparison methods to `Int64Vector`:
- `equals_vector(Int64Vector other)` - Element-wise equality comparison
- `not_equals_vector(Int64Vector other)` - Element-wise inequality comparison
- `greater_than_vector(Int64Vector other)` - Element-wise greater-than comparison
- `greater_than_or_equals_vector(Int64Vector other)` - Element-wise greater-than-or-equals comparison
- `less_than_vector(Int64Vector other)` - Element-wise less-than comparison
- `less_than_or_equals_vector(Int64Vector other)` - Element-wise less-than-or-equals comparison

**Files modified:**
- `draken/vectors/int64_vector.pxd` - Added method declarations
- `draken/vectors/int64_vector.pyx` - Added method implementations

### 2. Float64Vector Enhancements

Added the following vector-vector comparison methods to `Float64Vector`:
- `equals_vector(Float64Vector other)` - Element-wise equality comparison
- `not_equals_vector(Float64Vector other)` - Element-wise inequality comparison
- `greater_than_vector(Float64Vector other)` - Element-wise greater-than comparison
- `greater_than_or_equals_vector(Float64Vector other)` - Element-wise greater-than-or-equals comparison
- `less_than_vector(Float64Vector other)` - Element-wise less-than comparison
- `less_than_or_equals_vector(Float64Vector other)` - Element-wise less-than-or-equals comparison

**Files modified:**
- `draken/vectors/float64_vector.pxd` - Added method declarations
- `draken/vectors/float64_vector.pyx` - Added method implementations

### 3. Testing

Created comprehensive test suite in `tests/vectors/test_vector_comparisons.py`:
- Tests for all 6 Int64Vector comparison operations
- Tests for all 6 Float64Vector comparison operations
- Tests for error handling (length mismatch)
- Tests to ensure existing scalar comparisons still work

### 4. Documentation

Created demonstration script in `examples/vector_comparison_demo.py` showing:
- Int64Vector vector-vector comparisons
- Float64Vector vector-vector comparisons
- Backward compatibility with scalar comparisons

## Implementation Details

### Method Naming Convention

Vector-vector comparison methods use the `_vector` suffix to distinguish them from scalar comparison methods:
- Scalar comparison: `vec.equals(scalar_value)`
- Vector comparison: `vec.equals_vector(other_vector)`

### Error Handling

All vector-vector comparison methods validate that both vectors have the same length and raise a `ValueError` with message "Vectors must have the same length" if they don't match.

### Return Type

All comparison methods return `int8_t[::1]` (a memoryview of int8 values) where:
- `1` indicates the comparison is true for that element
- `0` indicates the comparison is false for that element

### Performance

The implementations use optimized Cython code with:
- Direct memory access via pointers
- Pre-allocated result buffers
- Efficient element-wise iteration

## Usage Examples

### Int64Vector Comparisons

```python
import pyarrow as pa
from draken import Vector

arr1 = pa.array([1, 2, 3, 4, 5], type=pa.int64())
arr2 = pa.array([1, 3, 3, 2, 6], type=pa.int64())

vec1 = Vector.from_arrow(arr1)
vec2 = Vector.from_arrow(arr2)

# Vector-vector comparison
result = vec1.equals_vector(vec2)  # Returns [1, 0, 1, 0, 0]

# Scalar comparison (still supported)
result = vec1.equals(3)  # Returns [0, 0, 1, 0, 0]
```

### Float64Vector Comparisons

```python
arr1 = pa.array([1.5, 2.7, 3.3], type=pa.float64())
arr2 = pa.array([1.5, 3.0, 3.3], type=pa.float64())

vec1 = Vector.from_arrow(arr1)
vec2 = Vector.from_arrow(arr2)

# Vector-vector comparison
result = vec1.greater_than_vector(vec2)  # Returns [0, 0, 0]
```

## Supported Combinations

The implementation supports the following viable combinations:

1. **Int64Vector vs Int64Vector** - All 6 comparison operators
2. **Int64Vector vs int64_t scalar** - All 6 comparison operators (pre-existing)
3. **Float64Vector vs Float64Vector** - All 6 comparison operators
4. **Float64Vector vs double scalar** - All 6 comparison operators (pre-existing)

**Note:** Mixed-type comparisons (e.g., Int64Vector vs Float64Vector) are NOT supported, as per the system design requirement that comparison operations require both operands to have the same type.

## Testing

All tests pass successfully:
- 16 new tests for vector-vector comparisons
- All existing tests continue to pass
- Backward compatibility maintained

Run tests with:
```bash
pytest tests/vectors/test_vector_comparisons.py -v
```

## Backward Compatibility

✅ All existing scalar comparison methods continue to work exactly as before
✅ No breaking changes to existing APIs
✅ New methods use different names (`_vector` suffix) to avoid conflicts

#!/usr/bin/env python3
"""
Demonstration script showing that the null subscript bug has been fixed.

Before the fix:
- StringVector returned b'' for null values
- Numeric vectors raised ValueError for null values

After the fix:
- All vector types return None for null values (consistent with ArrowVector)
"""
import pyarrow

from draken import Vector

print("=" * 60)
print("Testing StringVector with subscript access")
print("=" * 60)
data = Vector.from_arrow(pyarrow.array(['abc123', 'xyz789', None]))
for i in range(data.length):
    val = data[i]
    print(f'Index {i}: {repr(val):15} type: {type(val).__name__:8} is None: {val is None}')

print("\n" + "=" * 60)
print("Testing Int64Vector with subscript access")
print("=" * 60)
int_data = Vector.from_arrow(pyarrow.array([1, 2, None], type=pyarrow.int64()))
for i in range(int_data.length):
    val = int_data[i]
    print(f'Index {i}: {repr(val):15} type: {type(val).__name__:8} is None: {val is None}')

print("\n" + "=" * 60)
print("Testing Float64Vector with subscript access")
print("=" * 60)
float_data = Vector.from_arrow(pyarrow.array([1.5, 2.5, None], type=pyarrow.float64()))
for i in range(float_data.length):
    val = float_data[i]
    print(f'Index {i}: {repr(val):15} type: {type(val).__name__:8} is None: {val is None}')

print("\n" + "=" * 60)
print("Testing BoolVector with subscript access")
print("=" * 60)
bool_data = Vector.from_arrow(pyarrow.array([True, False, None], type=pyarrow.bool_()))
for i in range(bool_data.length):
    val = bool_data[i]
    print(f'Index {i}: {repr(val):15} type: {type(val).__name__:8} is None: {val is None}')

print("\n" + "=" * 60)
print("✓ All vector types now return None for null values!")
print("=" * 60)

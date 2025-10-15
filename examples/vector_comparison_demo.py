#!/usr/bin/env python
"""Demonstration of vector-vector comparison operations.

This script demonstrates the newly added vector-vector comparison operations
for Int64Vector and Float64Vector.
"""
import sys
from pathlib import Path

import pyarrow as pa

from draken import Vector

sys.path.insert(0, str(Path(__file__).parent.parent))



def main():
    print("=" * 70)
    print("Vector-Vector Comparison Operations Demo")
    print("=" * 70)
    print()
    
    # Demo 1: Int64Vector comparisons
    print("1. Int64Vector Comparisons")
    print("-" * 70)
    
    arr1 = pa.array([1, 2, 3, 4, 5], type=pa.int64())
    arr2 = pa.array([1, 3, 3, 2, 6], type=pa.int64())
    
    vec1 = Vector.from_arrow(arr1)
    vec2 = Vector.from_arrow(arr2)
    
    print(f"Vector 1: {list(arr1)}")
    print(f"Vector 2: {list(arr2)}")
    print()
    
    print(f"equals_vector:                {list(vec1.equals_vector(vec2))}")
    print(f"not_equals_vector:            {list(vec1.not_equals_vector(vec2))}")
    print(f"greater_than_vector:          {list(vec1.greater_than_vector(vec2))}")
    print(f"greater_than_or_equals_vector: {list(vec1.greater_than_or_equals_vector(vec2))}")
    print(f"less_than_vector:             {list(vec1.less_than_vector(vec2))}")
    print(f"less_than_or_equals_vector:   {list(vec1.less_than_or_equals_vector(vec2))}")
    print()
    
    # Demo 2: Float64Vector comparisons
    print("2. Float64Vector Comparisons")
    print("-" * 70)
    
    arr1 = pa.array([1.5, 2.7, 3.3, 4.1, 5.9], type=pa.float64())
    arr2 = pa.array([1.5, 3.0, 3.3, 2.0, 6.0], type=pa.float64())
    
    vec1 = Vector.from_arrow(arr1)
    vec2 = Vector.from_arrow(arr2)
    
    print(f"Vector 1: {[float(x) for x in arr1]}")
    print(f"Vector 2: {[float(x) for x in arr2]}")
    print()
    
    print(f"equals_vector:                {list(vec1.equals_vector(vec2))}")
    print(f"not_equals_vector:            {list(vec1.not_equals_vector(vec2))}")
    print(f"greater_than_vector:          {list(vec1.greater_than_vector(vec2))}")
    print(f"greater_than_or_equals_vector: {list(vec1.greater_than_or_equals_vector(vec2))}")
    print(f"less_than_vector:             {list(vec1.less_than_vector(vec2))}")
    print(f"less_than_or_equals_vector:   {list(vec1.less_than_or_equals_vector(vec2))}")
    print()
    
    # Demo 3: Scalar comparisons still work
    print("3. Scalar Comparisons (still supported)")
    print("-" * 70)
    
    arr = pa.array([1, 2, 3, 4, 5], type=pa.int64())
    vec = Vector.from_arrow(arr)
    
    print(f"Vector: {list(arr)}")
    print(f"Comparing with scalar value 3:")
    print()
    
    print(f"equals(3):                {list(vec.equals(3))}")
    print(f"not_equals(3):            {list(vec.not_equals(3))}")
    print(f"greater_than(3):          {list(vec.greater_than(3))}")
    print(f"greater_than_or_equals(3): {list(vec.greater_than_or_equals(3))}")
    print(f"less_than(3):             {list(vec.less_than(3))}")
    print(f"less_than_or_equals(3):   {list(vec.less_than_or_equals(3))}")
    print()
    
    print("=" * 70)
    print("Demo complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()

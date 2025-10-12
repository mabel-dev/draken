#!/usr/bin/env python
"""
Example usage of the Draken operation dispatch system.

This script demonstrates how to use the operation dispatch system to
check if operations are supported for different type combinations.
"""

from draken.core.ops import (
    get_op,
    get_operation_enum,
    TYPE_INT64,
    TYPE_FLOAT64,
    TYPE_BOOL,
    TYPE_STRING,
)


def main():
    """Demonstrate operation dispatch usage."""
    
    print("=" * 70)
    print("Draken Operation Dispatch System - Example Usage")
    print("=" * 70)
    print()
    
    # Get operation enums
    print("1. Getting Operation Enums")
    print("-" * 70)
    equals_op = get_operation_enum('equals')
    add_op = get_operation_enum('add')
    and_op = get_operation_enum('and')
    
    print(f"  equals:  {equals_op}")
    print(f"  add:     {add_op}")
    print(f"  and:     {and_op}")
    print()
    
    # Test comparison operations
    print("2. Testing Comparison Operations (using get_op)")
    print("-" * 70)
    
    # Supported: int64 == int64
    result = get_op(TYPE_INT64, False, TYPE_INT64, True, 'equals')
    print(f"  int64 == int64 (scalar):  {result} (compatible)")
    
    # Unsupported: int64 == float64
    result = get_op(TYPE_INT64, False, TYPE_FLOAT64, True, 'equals')
    print(f"  int64 == float64:         {result} (incompatible)")
    
    # Supported: float64 == float64
    result = get_op(TYPE_FLOAT64, False, TYPE_FLOAT64, False, 'equals')
    print(f"  float64 == float64:       {result} (compatible)")
    print()
    
    # Test arithmetic operations
    print("3. Testing Arithmetic Operations (using get_op)")
    print("-" * 70)
    
    # Supported: int64 + int64
    result = get_op(TYPE_INT64, False, TYPE_INT64, False, 'add')
    print(f"  int64 + int64:      {result} (compatible)")
    
    # Supported: float64 + float64
    result = get_op(TYPE_FLOAT64, True, TYPE_FLOAT64, False, 'add')
    print(f"  float64 + float64:  {result} (compatible)")
    
    # Unsupported: string + string
    result = get_op(TYPE_STRING, False, TYPE_STRING, False, 'add')
    print(f"  string + string:    {result} (incompatible)")
    print()
    
    # Test boolean operations
    print("4. Testing Boolean Operations (using get_op)")
    print("-" * 70)
    
    # Supported: bool AND bool
    result = get_op(TYPE_BOOL, False, TYPE_BOOL, False, 'and')
    print(f"  bool AND bool:   {result} (compatible)")
    
    # Unsupported: int64 AND int64
    result = get_op(TYPE_INT64, False, TYPE_INT64, False, 'and')
    print(f"  int64 AND int64: {result} (incompatible)")
    print()
    
    # Test scalarity combinations
    print("5. Testing Scalarity Combinations (using get_op)")
    print("-" * 70)
    
    # All combinations of vector/scalar with int64 addition
    combos = [
        (False, False, "vector + vector"),
        (False, True,  "vector + scalar"),
        (True,  False, "scalar + vector"),
        (True,  True,  "scalar + scalar"),
    ]
    
    for left_scalar, right_scalar, desc in combos:
        result = get_op(TYPE_INT64, left_scalar, TYPE_INT64, right_scalar, 'add')
        status = "compatible" if result is None else "has function"
        print(f"  {desc:20s}: {result} ({status})")
    print()
    
    # Test error handling
    print("6. Testing Error Handling")
    print("-" * 70)
    
    try:
        invalid_op = get_operation_enum('invalid_operation')
        print(f"  Got operation: {invalid_op}")
    except ValueError as e:
        print(f"  ✓ Correctly raised ValueError: {e}")
    print()
    
    print("=" * 70)
    print("Example completed successfully!")
    print("=" * 70)


if __name__ == '__main__':
    main()

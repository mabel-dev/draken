"""
Demonstration of StringVector C-level API improvements.

This example shows:
1. C-level iterator for high-performance kernel operations
2. Raw pointer append methods for zero-copy-friendly building
"""

from draken.vectors.string_vector import StringVector
from draken.vectors.string_vector import StringVectorBuilder


def demo_basic_operations():
    """Basic StringVector operations."""
    print("=" * 60)
    print("Basic StringVector Operations")
    print("=" * 60)
    
    # Create a builder
    builder = StringVectorBuilder.with_estimate(5, 20)
    builder.append(b"hello")
    builder.append(b"world")
    builder.append_null()
    builder.append(b"from")
    builder.append(b"draken")
    
    vec = builder.finish()
    
    print(f"Vector length: {len(vec)}")
    print(f"Null count: {vec.null_count}")
    print("\nValues:")
    for i, val in enumerate(vec):
        print(f"  [{i}]: {val}")
    
    return vec


def demo_python_iteration():
    """Standard Python iteration (creates bytes objects)."""
    print("\n" + "=" * 60)
    print("Python Iteration (bytes objects)")
    print("=" * 60)
    
    builder = StringVectorBuilder.with_estimate(3, 20)
    builder.append(b"alpha")
    builder.append(b"beta")
    builder.append(b"gamma")
    vec = builder.finish()
    
    total_len = 0
    for val in vec:
        if val is not None:
            total_len += len(val)
            print(f"  {val} -> length {len(val)}")
    
    print(f"Total length: {total_len}")


def demo_builder_api():
    """Demonstrate builder API with exact capacity."""
    print("\n" + "=" * 60)
    print("StringVectorBuilder API")
    print("=" * 60)
    
    # Method 1: with_counts (strict capacity)
    print("\n1. Builder with exact byte count:")
    builder = StringVectorBuilder.with_counts(3, 15)  # "abc" + "defgh" + "ij" = 15 bytes
    print(f"   Capacity: {builder.bytes_capacity}, Used: {builder.bytes_used}")
    
    builder.append(b"abc")
    print(f"   After append: Used: {builder.bytes_used}, Remaining: {builder.remaining_bytes}")
    
    builder.append(b"defgh")
    print(f"   After append: Used: {builder.bytes_used}, Remaining: {builder.remaining_bytes}")
    
    builder.append(b"ij")
    print(f"   After append: Used: {builder.bytes_used}, Remaining: {builder.remaining_bytes}")
    
    vec = builder.finish()
    print(f"   Finished: {list(vec)}")
    
    # Method 2: with_estimate (resizable)
    print("\n2. Builder with estimated capacity (resizable):")
    builder = StringVectorBuilder.with_estimate(3, 5)  # Estimate 5 bytes per string
    print(f"   Initial capacity: {builder.bytes_capacity}")
    
    builder.append(b"short")
    builder.append(b"this is a much longer string that will trigger resize")
    builder.append(b"ok")
    
    print(f"   Final capacity: {builder.bytes_capacity}")
    vec = builder.finish()
    print(f"   Values: {[str(v) for v in vec]}")


def demo_null_handling():
    """Demonstrate null handling in builder."""
    print("\n" + "=" * 60)
    print("Null Handling")
    print("=" * 60)
    
    builder = StringVectorBuilder.with_estimate(5, 20)
    builder.append(b"first")
    builder.append_null()
    builder.append(b"third")
    builder.append_null()
    builder.append(b"fifth")
    
    vec = builder.finish()
    
    print(f"Vector: {vec}")
    print(f"Null count: {vec.null_count}")
    print("\nIteration:")
    for i, val in enumerate(vec):
        if val is None:
            print(f"  [{i}]: NULL")
        else:
            print(f"  [{i}]: {val}")


def demo_view_api():
    """Demonstrate the view API for zero-copy access."""
    print("\n" + "=" * 60)
    print("StringVector View API")
    print("=" * 60)
    
    builder = StringVectorBuilder.with_estimate(3, 20)
    builder.append(b"view")
    builder.append(b"test")
    builder.append_null()
    vec = builder.finish()
    
    view = vec.view()
    
    print("Using view for random access:")
    for i in range(len(vec)):
        if view.is_null(i):
            print(f"  [{i}]: NULL")
        else:
            ptr = view.value_ptr(i)
            length = view.value_len(i)
            print(f"  [{i}]: ptr=0x{ptr:x}, len={length}")


def demo_operations():
    """Demonstrate vector operations."""
    print("\n" + "=" * 60)
    print("Vector Operations")
    print("=" * 60)
    
    builder = StringVectorBuilder.with_estimate(5, 30)
    builder.append(b"apple")
    builder.append(b"banana")
    builder.append(b"apple")
    builder.append(b"cherry")
    builder.append(b"apple")
    vec = builder.finish()
    
    # Equality mask
    print("\n1. Equality mask (== b'apple'):")
    mask = vec.equals(b"apple")
    print(f"   Mask: {list(mask)}")
    
    # Hashing
    print("\n2. Hash values:")
    hashes = vec.hash()
    for i, h in enumerate(hashes):
        print(f"   [{i}]: {vec[i]} -> 0x{h:016x}")
    
    # Take operation
    print("\n3. Take operation (indices [0, 2, 4]):")
    import numpy as np
    indices = np.array([0, 2, 4], dtype=np.int32)
    taken = vec.take(indices)
    print(f"   Result: {list(taken)}")


def demo_arrow_interop():
    """Demonstrate Arrow interoperability."""
    print("\n" + "=" * 60)
    print("Arrow Interoperability")
    print("=" * 60)
    
    try:
        import pyarrow as pa

        # Create a StringVector
        builder = StringVectorBuilder.with_estimate(4, 25)
        builder.append(b"arrow")
        builder.append(b"interop")
        builder.append_null()
        builder.append(b"test")
        vec = builder.finish()
        
        # Convert to Arrow
        arrow_array = vec.to_arrow()
        print(f"Arrow array: {arrow_array}")
        print(f"Arrow type: {arrow_array.type}")
        print(f"Arrow null count: {arrow_array.null_count}")
        
    except ImportError:
        print("PyArrow not available, skipping Arrow interop demo")


if __name__ == "__main__":
    demo_basic_operations()
    demo_python_iteration()
    demo_builder_api()
    demo_null_handling()
    demo_view_api()
    demo_operations()
    demo_arrow_interop()
    
    print("\n" + "=" * 60)
    print("All demos completed!")
    print("=" * 60)
    print("\nNote: For C-level iterator examples, see the Cython-based")
    print("examples in examples/c_iterator_demo.pyx")

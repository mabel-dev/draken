# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# distutils: language=c++

"""
This is not a general perpose Bloom Filter, if used outside of Draken, it may not
perform entirely as expected.
"""

from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy
from cpython cimport PyUnicode_AsUTF8String

from .murmurhash3_32 cimport cy_murmurhash3

# Define constants for the fixed size
BIT_ARRAY_SIZE = 64 * 1024  # 64 KB = 512 Kbits
BYTE_ARRAY_SIZE = BIT_ARRAY_SIZE // 8

cdef class BloomFilter:
#    cdef unsigned char* bit_array  # defined in the .pxd file only

    def __cinit__(self):
        # Allocate memory for the bit array and initialize to 0
        self.bit_array = <unsigned char*>malloc(BYTE_ARRAY_SIZE)
        if not self.bit_array:
            raise MemoryError("Failed to allocate memory for the bit array.")
        memset(self.bit_array, 0, BYTE_ARRAY_SIZE)

    def __dealloc__(self):
        if self.bit_array:
            free(self.bit_array)

    cpdef void add(self, long item):
        """Add an item to the Bloom filter"""
        h1 = item % BIT_ARRAY_SIZE
        # Apply the golden ratio to the item and use modulo to wrap within the size of the bit array
        h2 = <long>(item * 1.618033988749895) % BIT_ARRAY_SIZE
        # Set bits using bitwise OR
        self.bit_array[h1 // 8] |= 1 << (h1 % 8)
        self.bit_array[h2 // 8] |= 1 << (h2 % 8)

    cpdef int possibly_contains(self, long item):
        """Check if the item might be in the set"""
        h1 = item % BIT_ARRAY_SIZE
        # Apply the golden ratio to the item and use modulo to wrap within the size of the bit array
        h2 = <long>(item * 1.618033988749895) % BIT_ARRAY_SIZE
        # Check bits using bitwise AND
        return (self.bit_array[h1 // 8] & (1 << (h1 % 8))) and \
               (self.bit_array[h2 // 8] & (1 << (h2 % 8)))

    cpdef memoryview serialize(self):
        """Serialize the Bloom filter to a memory view"""
        return memoryview(self.bit_array[:BYTE_ARRAY_SIZE])

cpdef BloomFilter deserialize(const unsigned char* data):
    """Deserialize a memory view to a Bloom filter"""
    bf = BloomFilter()
    memcpy(bf.bit_array, data, BYTE_ARRAY_SIZE)
    return bf


cpdef create_bloom_filter(list keys):
    cdef bytes key_bytes
    cdef int key_len
    bf = BloomFilter()
    for key_bytes in keys:
        key_len = len(key_bytes)  # Length of the key
        hashed_key = cy_murmurhash3(<const void*>key_bytes, key_len, 703115)
        bf.add(hashed_key)
    return bf
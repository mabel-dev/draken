# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# distutils: language=c++

"""
This is not a general perpose Bloom Filter, if used outside of Draken, it may not
perform entirely as expected as it is optimized for a specific configuration. 

We use 64kb to create a 512k slot bit array. We then use one hash to create two positions using the golden ratio.

On 50k items, we get approximately 3% FP rate.
"""

from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy
from cpython cimport PyUnicode_AsUTF8String

from .murmurhash3_32 cimport cy_murmurhash3

cdef uint32_t BYTE_ARRAY_SIZE = 64 * 1024  # 64 KB
cdef uint32_t BIT_ARRAY_SIZE = BYTE_ARRAY_SIZE << 3 # 512 Kbits


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

    cpdef void add(self, bytes member):
        """Add an item to the Bloom filter"""
        item = cy_murmurhash3(<char*>member, len(member), 0)
        h1 = item % BIT_ARRAY_SIZE
        # Apply the golden ratio to the item and use modulo to wrap within the size of the bit array
        h2 = <long>(item * 1.618033988) % BIT_ARRAY_SIZE  # 1.618033988749895
        # Set bits using bitwise OR
        self.bit_array[h1 >> 3] |= 1 << (h1 % 8)
        self.bit_array[h2 >> 3] |= 1 << (h2 % 8)

    cpdef int possibly_contains(self, bytes member):
        """Check if the item might be in the set"""
        item = cy_murmurhash3(<char*>member, len(member), 0)
        h1 = item % BIT_ARRAY_SIZE
        # Apply the golden ratio to the item and use modulo to wrap within the size of the bit array
        h2 = <long>(item * 1.618033988) % BIT_ARRAY_SIZE
        # Check bits using bitwise AND
        return (self.bit_array[h1 >> 3] & (1 << (h1 % 8))) and \
               (self.bit_array[h2 >> 3] & (1 << (h2 % 8)))

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
    bf = BloomFilter()
    for key_bytes in keys:
        bf.add(key_bytes)
    return bf
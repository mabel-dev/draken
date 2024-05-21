# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# distutils: language=c++

BIT_ARRAY_SIZE = 64 * 1024  # 64 KB = 512 Kbits
BYTE_ARRAY_SIZE = BIT_ARRAY_SIZE // 8

# Declaration of the BloomFilter class
cdef class BloomFilter:
    cdef unsigned char* bit_array

    cpdef void add(self, long item)
    cpdef int possibly_contains(self, long item)
    cpdef memoryview serialize(self)

cpdef BloomFilter deserialize(const unsigned char* data)
cpdef create_bloom_filter(list keys)

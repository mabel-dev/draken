# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# distutils: language=c++

from libc.stdint cimport uint32_t

# Declaration of the BloomFilter class
cdef class BloomFilter:
    cdef unsigned char* bit_array

    cpdef void add(self, bytes member)
    cpdef int possibly_contains(self, bytes member)
    cpdef memoryview serialize(self)

cpdef BloomFilter deserialize(const unsigned char* data)
cpdef create_bloom_filter(list keys)

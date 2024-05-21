from libc.stdint import int64_t
from libc.stdint import uint32_t


cdef uint32_t cy_murmurhash3(const void *key, uint32_t len, uint32_t seed)

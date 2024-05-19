# MurmurHash3 implementation for better performance
cdef inline uint32_t murmurhash3_32(const void *key, int len, uint32_t seed):
    cdef uint32_t c1 = 0xcc9e2d51
    cdef uint32_t c2 = 0x1b873593
    cdef uint32_t r1 = 15
    cdef uint32_t r2 = 13
    cdef uint32_t m = 5
    cdef uint32_t n = 0xe6546b64

    cdef const unsigned char *data = <const unsigned char *>key
    cdef const int nblocks = len // 4
    cdef uint32_t h1 = seed
    cdef uint32_t k1 = 0

    # body
    cdef const uint32_t *blocks = <const uint32_t *>data
    for i in range(nblocks):
        k1 = blocks[i]

        k1 *= c1
        k1 = (k1 << r1) | (k1 >> (32 - r1))
        k1 *= c2

        h1 ^= k1
        h1 = (h1 << r2) | (h1 >> (32 - r2))
        h1 = h1 * m + n

    # tail
    cdef const unsigned char *tail = <const unsigned char *>(data + nblocks * 4)
    cdef uint32_t k1_ = 0

    if len & 3 == 3:
        k1_ ^= tail[2] << 16
    if len & 3 >= 2:
        k1_ ^= tail[1] << 8
    if len & 3 >= 1:
        k1_ ^= tail[0]
        k1_ *= c1
        k1_ = (k1_ << r1) | (k1_ >> (32 - r1))
        k1_ *= c2
        h1 ^= k1_

    # finalization
    h1 ^= len
    h1 ^= (h1 >> 16)
    h1 *= 0x85ebca6b
    h1 ^= (h1 >> 13)
    h1 *= 0xc2b2ae35
    h1 ^= (h1 >> 16)

    return h1
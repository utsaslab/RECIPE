#ifndef __HOT_COMMONS__PERSIST__
#define __HOT_COMMONS__PERSIST__

namespace hot { namespace commons {

inline void mfence()
{
    asm volatile("sfence":::"memory");
}

inline void clflush(char *data, int len, bool front, bool back)
{
    unsigned long CACHE_LINE_SIZE_ = 64;
    volatile char *ptr = (char *)((unsigned long)data & ~(CACHE_LINE_SIZE_ - 1));

    if (front) mfence();
    for (; ptr < data+len; ptr += CACHE_LINE_SIZE_){
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
    }
    if (back) mfence();
}

inline void movnt64(uint64_t *dest, uint64_t const &src, bool front, bool back)
{
    assert(((uint64_t)dest & 7) == 0);
    if (front) mfence();
    _mm_stream_si64((long long int *)dest, *(long long int *)&src);
    if (back) mfence();
}

}}

#endif

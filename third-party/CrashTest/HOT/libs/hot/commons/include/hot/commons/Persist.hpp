#ifndef __HOT_COMMONS__PERSIST__
#define __HOT_COMMONS__PERSIST__

namespace hot { namespace commons {

static inline void cpu_pause()
{
    __asm__ volatile ("pause" ::: "memory");
}

static inline unsigned long read_tsc(void)
{
    unsigned long var;
    unsigned int hi, lo;

    asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long long int) hi << 32) | lo;

    return var;
}

inline void mfence()
{
    asm volatile("mfence":::"memory");
}

inline void clflush(char *data, int len)
{
    unsigned long write_latency_in_ns = 0, CPU_FREQ_MHZ_ = 2100,
                  CACHE_LINE_SIZE_ = 64;
    volatile char *ptr = (char *)((unsigned long)data & ~(CACHE_LINE_SIZE_ - 1));
    for (; ptr < data+len; ptr += CACHE_LINE_SIZE_){
        unsigned long etsc = read_tsc() +
            (unsigned long)(write_latency_in_ns * CPU_FREQ_MHZ_/1000);
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
        while (read_tsc() < etsc) cpu_pause();
    }
}

}}

#endif

#ifndef UTIL_PERSIST_H_
#define UTIL_PERSIST_H_

#include <cstdlib>
#include <stdint.h>

static uint64_t CPU_FREQ_MHZ = 2100;  // cat /proc/cpuinfo
#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))
#define kCacheLineSize (64)

static uint64_t kWriteLatencyInNS = 0;
static uint64_t clflushCount = 0;

static inline void CPUPause(void) {
  __asm__ volatile("pause":::"memory");
}

static inline unsigned long ReadTSC(void) {
  unsigned long var;
  unsigned int hi, lo;
  asm volatile("rdtsc":"=a"(lo),"=d"(hi));
  var = ((unsigned long long int) hi << 32) | lo;
  return var;
}

inline void mfence(void) {
  asm volatile("mfence":::"memory");
}

inline void clflush(char* data, size_t len) {
  volatile char *ptr = (char*)((unsigned long)data & (~(kCacheLineSize-1)));
  mfence();
  for (; ptr < data+len; ptr+=kCacheLineSize) {
    unsigned long etcs = ReadTSC() + (unsigned long) (kWriteLatencyInNS*CPU_FREQ_MHZ/1000);
#ifdef CLFLUSH
    asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char*)ptr));
#elif CLWB
    asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
    while (ReadTSC() < etcs) CPUPause();
    clflushCount++;
  }
  mfence();
}


#endif  // UTIL_PERSIST_H_

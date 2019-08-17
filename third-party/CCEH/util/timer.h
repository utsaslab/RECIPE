#ifndef UTIL_TIMER_H_
#define UTIL_TIMER_H_

#include <time.h>

class Timer_ {
  public:
    Timer_(void): elapsed{0} {}

    void Start(void) {
      clock_gettime(CLOCK_MONOTONIC, &start);
    }
    void Stop(void) {
      clock_gettime(CLOCK_MONOTONIC, &end);
      elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
    }
    size_t Get(void) {
      return elapsed;
    }
    double GetSeconds(void) {
      return elapsed / 1000000000.0;
    }
    size_t Now(void) {
      clock_gettime(CLOCK_MONOTONIC, &now);
      return now.tv_sec * 1000000000 + now.tv_nsec;
    }
    void Accumulate(void) {
      clock_gettime(CLOCK_MONOTONIC, &end);
      elapsed += (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
    }

  private:
    struct timespec start, end, now;
    size_t elapsed;
};

#endif  // UTIL_TIMER_H_

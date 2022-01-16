#define _GNU_SOURCE 
#include <sys/mman.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <sys/types.h>
#include <linux/perf_event.h>
#include <linux/hw_breakpoint.h>
#include <sys/ioctl.h>
#include <asm/unistd.h>
#include <inttypes.h>
#include <sched.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <assert.h>

int perfFileDesc[4];
struct perf_event_attr perfEventAttr[4];
uint64_t cache_id[2];
uint64_t cache_op_id[2]; 
uint64_t cache_op_result_id;

void create_perf_event_attr(struct perf_event_attr *pe, uint64_t id, uint64_t op_id, uint64_t op_result_id) {
	
	memset(pe, 0, sizeof(struct perf_event_attr));
	pe->type = PERF_TYPE_HW_CACHE;
	pe->size = sizeof(struct perf_event_attr);
	pe->config = (id) | (op_id << 8) | (op_result_id << 16);
	pe->disabled = 1;
	pe->exclude_kernel = 1;
	pe->exclude_hv = 1;

}



static long perf_event_open(struct perf_event_attr *hw_event, pid_t pid,
                int cpu, int group_fd, unsigned long flags)
{
    int ret;

    ret = syscall(__NR_perf_event_open, hw_event, pid, cpu,
                   group_fd, flags);
    return ret;
}

static int open_perf_event() {
	
	int i,j,k;

	cache_id[0] = PERF_COUNT_HW_CACHE_LL;
	cache_id[1] = PERF_COUNT_HW_CACHE_DTLB;
	cache_op_id[0] = PERF_COUNT_HW_CACHE_OP_WRITE;
	cache_op_id[1] = PERF_COUNT_HW_CACHE_OP_READ;
	cache_op_result_id = PERF_COUNT_HW_CACHE_RESULT_MISS;

	int ctr = 0;

	for(i=0; i<2; i++) {
	  	for(j=0; j<2; j++) {
	  		create_perf_event_attr(&perfEventAttr[ctr], cache_id[i], cache_op_id[j], cache_op_result_id);
	  		perfFileDesc[ctr] = perf_event_open(&perfEventAttr[ctr], 0, -1, -1, 0);
	  		if(perfFileDesc[ctr] == -1) {
	    			perror("file descriptor not opened");
	    			exit(EXIT_FAILURE);
	  		}
			ctr++;
	  	}
	}
}

static int do_ioctl_call(unsigned long request) {
	
	int i;
	for(i=0; i<4; i++) {
	  if((ioctl(perfFileDesc[i], request, 0)) == -1) {
	    perror("ioctl not working");
	    exit(EXIT_FAILURE);
	  }
	}
}

static int close_perf_desc() {
	
	int i;

	for(i=0; i<4; i++) {
	  if(close(perfFileDesc[i]) == -1) {
	    perror("filedesc close not working");
	    exit(EXIT_FAILURE);
	  }
	}
}

long long print_event() {
	
	long long count;
	long long cache_misses, read_cache_misses, write_cache_misses;
	int i;
	
	for(i=0; i<4; i++) {
	  read(perfFileDesc[i], &count, sizeof(long long));
	  if(i == 0) { 
	    printf("%s: Write L3 Cache Misses = %lld\n", __func__, count/2);
	    write_cache_misses = count/2;
	  } else if(i == 1) {
	    printf("%s: Read L3 Cache Misses = %lld\n", __func__, count);
	    read_cache_misses = count;
	  } else if(i == 2) {
	    printf("%s: Write TLB Misses = %lld\n", __func__, count);
	  } else if(i == 3) {
	    printf("%s: Read TLB Misses = %lld\n", __func__, count);
	  }
	}

	cache_misses = read_cache_misses + write_cache_misses;
	return cache_misses;
}


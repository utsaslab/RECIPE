#include <cassert>
#include <asm/unistd.h>
#include <chrono>
#include <cstring>
#include <cstring>
#include <iostream>
#include <linux/perf_event.h>
#include <string>
#include <sys/ioctl.h>
#include <unistd.h>
#include <vector>
#include <iomanip>
#include <map>

/**
 * Tool for collecting cpu performance counters.
 *
 * @author leis@in.tum.de
 */
struct PerfEvent {

	struct event {
		struct read_format {
			uint64_t value;
			uint64_t time_enabled;
			uint64_t time_running;
			uint64_t id;
		};

		perf_event_attr pe;
		int fd;
		read_format prev;
		read_format data;

		double readCounter() {
			double multiplexingCorrection = static_cast<double>(data.time_enabled - prev.time_enabled) / (data.time_running - prev.time_running);
			return (data.value - prev.value) * multiplexingCorrection;
		}
	};

	std::vector<event> events;
	std::vector<std::string> names;
	std::chrono::time_point<std::chrono::steady_clock> startTime;
	std::chrono::time_point<std::chrono::steady_clock> stopTime;

	PerfEvent() {
		registerCounter("cycles", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);
		registerCounter("instructions", PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);
		registerCounter("L1-misses", PERF_TYPE_HW_CACHE, PERF_COUNT_HW_CACHE_L1D|(PERF_COUNT_HW_CACHE_OP_READ<<8)|(PERF_COUNT_HW_CACHE_RESULT_MISS<<16));
		registerCounter("LLC-misses", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES);
		//registerCounter("branch-misses", PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES);

		registerCounter("task-clock", PERF_TYPE_SOFTWARE, PERF_COUNT_SW_TASK_CLOCK);
		//registerCounter("page-faults", PERF_TYPE_SOFTWARE, PERF_COUNT_SW_PAGE_FAULTS);
		//registerCounter("context-switches", PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CONTEXT_SWITCHES);

		for (unsigned i=0; i<events.size(); i++) {
			auto& event = events[i];
			event.fd = syscall(__NR_perf_event_open, &event.pe, 0, -1, -1, 0);
			if (event.fd < 0)
				std::cerr << "Error reading counter on initialization" << names[i] << std::endl; // unreachable if perf works correctly
		}
	}

	void registerCounter(const std::string& name, uint64_t type, uint64_t eventID) {
		names.push_back(name);
		events.push_back(event());
		auto& event = events.back();
		auto& pe = event.pe;
		memset(&pe, 0, sizeof(struct perf_event_attr));
		pe.type = type;
		pe.size = sizeof(struct perf_event_attr);
		pe.config = eventID;
		pe.disabled = true;
		pe.inherit = 1;
		pe.inherit_stat = 0;
		pe.exclude_kernel = false;
		pe.exclude_hv = false;
		pe.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
	}

	void startCounters() {
		for (unsigned i=0; i<events.size(); i++) {
			auto& event = events[i];
			ioctl(event.fd, PERF_EVENT_IOC_RESET, 0);
			ioctl(event.fd, PERF_EVENT_IOC_ENABLE, 0);
			if (read(event.fd, &event.prev, sizeof(uint64_t) * 3) != sizeof(uint64_t) * 3)
				std::cerr << "Error reading counter " << names[i] << std::endl; // unreachable if perf works correctly
		}
		startTime = std::chrono::steady_clock::now();
	}

	~PerfEvent() {
		for (auto& event : events) {
			close(event.fd);
		}
	}

	void stopCounters() {
		stopTime = std::chrono::steady_clock::now();
		for (unsigned i=0; i<events.size(); i++) {
			auto& event = events[i];
			if (read(event.fd, &event.data, sizeof(uint64_t) * 3) != sizeof(uint64_t) * 3)
				std::cerr << "Error reading counter " << names[i] << std::endl; // unreachable if perf works correctly
			ioctl(event.fd, PERF_EVENT_IOC_DISABLE, 0);
		}
	}

	double getDuration() {
		return std::chrono::duration<double>(stopTime - startTime).count();
	}

	double getIPC() {
		return getCounter("instructions") / getCounter("cycles");
	}

	double getCPUs() {
		return getCounter("task-clock") / (getDuration() * 1e9);
	}

	double getGHz() {
		return getCounter("cycles")/1e9/getDuration();
	}

	double getCounter(const std::string& name) {
		for (unsigned i=0; i<events.size(); i++) {
			if (names[i] == name) {
				return events[i].readCounter();
			}
		}
		assert(false);
		return -1;
	}

	void printReport(std::ostream& out, uint64_t normalizationConstant) {
		std::streamsize defaultPrecision = out.precision();

		// print duration
		out << std::fixed << std::setprecision(6);
		out << getDuration() << " s execution";

		// print all metrics
		out << std::fixed << std::setprecision(2);
		for (unsigned i=0; i<events.size(); i++)
			out << ", " << events[i].readCounter()/normalizationConstant << " " << names[i];
		out << ", " << normalizationConstant << " scale";

		// derived metrics
		out << ", " << getIPC() << " IPC";
		out << ", " << getCPUs() << " CPUs";
		out << ", " << getGHz() << " GHz";

		out << std::defaultfloat << std::setprecision(defaultPrecision);
	}

	std::map<std::string, double> getRawResults(uint64_t normalizationConstant) {
		std::map<std::string, double> results;

		results["duration"] = getDuration();
		results["IPC"] = getIPC();
		results["GHz"] = getGHz();
		results["CPUs"] = getCPUs();

		for (unsigned i=0; i<events.size(); i++) {
			results[names[i]] = events[i].readCounter()/normalizationConstant;
		}

		return results;
	};

};


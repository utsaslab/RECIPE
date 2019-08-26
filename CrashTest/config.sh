echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
watch -n 0 "cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq"

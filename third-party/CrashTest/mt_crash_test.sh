#!/bin/bash

if [ $# -ne 2 ]; then
	echo "Enter output directory name and keytype[string | randint]"
	exit 1
fi 

run_name=$1
key_type=$2

DIR='results_crash_test'$run_name
rm -rf ./results/$DIR

error=0
mkdir -p ./results/$DIR

cd ./CLHT
bash compile.sh lb
cd ..

mkdir ./build
cd ./build
rm -rf *
cmake ..
make -j
cd ..

if [ "$key_type" == "string" ]; then

#for index in art hot masstree bwtree fastfair 
for index in fastfair art hot masstree bwtree
do
	for crash in 1
	do
		for threads in 16
		do
			for load_size in 100 1000 10000
			do 
				for run_size in 10000
				do
					for workload in a
					do
						for i in {1..1000}
						do
							./build/mtcrash ${index} ${workload} string uniform ${threads} ${load_size} ${run_size} ${crash} &>> ./results/${DIR}/${index}${crash}.log 2>&1
							status=$?
							if [ $status == 1 ]; then
								((error++))
							fi
                            if [ $status == 124 ]; then
                                ((error++))
                            fi
						done
					done
				done
			done
		done
		echo "Total errors = " $error >> ./results/${DIR}/${index}${crash}.log
        echo "Total errors for " $index " with crash test is " $error
        error=0
	done
done

elif [ "$key_type" == "randint" ]; then
for index in cceh clht
do
    for crash in 1
    do
        for threads in 16
        do
            for load_size in 100 1000 10000
            do 
                for run_size in 10000
                do
    				for workload in a
                    do
                        for i in {1..1000}
                        do  
                        	timeout 0.5s ./build/mtcrash ${index} ${workload} randint uniform ${threads} ${load_size} ${run_size} ${crash} &>> ./results/${DIR}/${index}${crash}.log 2>&1
                            status=$?
							if [ $status == 1 ]; then
								((error++))
							fi
                            if [ $status == 124 ]; then
                                ((error++))
                            fi
                        done
                    done
                done
            done
        done
		echo "Total errors = " $error >> ./results/${DIR}/${index}${crash}.log
		echo "Total errors for " $index " with crash test is " $error
		error=0
    done
done

fi

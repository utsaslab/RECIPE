# index-microbench

## Generate Workloads ## 

1. Download [YCSB](https://github.com/brianfrankcooper/YCSB/releases/latest)

   ```sh
   curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.11.0/ycsb-0.11.0.tar.gz
   tar xfvz ycsb-0.11.0.tar.gz
   mv ycsb-0.11.0 YCSB
   ``` 

2. Create Workload Spec 
 
   The default workload a-f are in ./workload_spec 
 
   You can of course generate your own spec and put it in this folder. 

3. Modify workload_config.inp

   1st arg: workload spec file name
   2nd arg: key type (randint = random integer; monoint = monotonically increasing integer; email = email keys with host name reversed)

4. Generate

   ```sh
   make generate_workload
   ```

   The generated workload files will be in ./workloads

5. NOTE: To generate email-key workloads, you need an email list (list.txt)# index-microbench 

## Publications ##

This index benchmarking framework has been used as the experimental environment for the following research papers:

Ziqi Wang, Andrew Pavlo, Hyeontaek Lim, Viktor Leis, Huanchen Zhang,
Michael Kaminsky, and David G. Andersen. 2018. **Building a Bw-Tree Takes
More Than Just Buzz Words.** In Proceedings of 2018 International Conference
on Management of Data (SIGMOD’18). ACM, New York, NY, USA, 16 pages.
https://doi.org/10.1145/3183713.3196895

$ git clone https://github.com/libuv/libuv.git  
$ sh autogen.sh  
$ ./configure  
$ make  
$ make check  
$ sudo make install  

$ git clone https://github.com/redis/hiredis.git  
$ cd hiredis  
$ make  
$ make install  

$ git clone https://github.com/sewenew/redis-plus-plus.git  
$ cd redis-plus-plus  
$ mkdir build  
$ cd build  
$ cmake -DCMAKE_PREFIX_PATH=/installation/path/to/libuv/and/hiredis -DREDIS_PLUS_PLUS_BUILD_ASYNC=libuv ..  
$ make  
$ sudo make install  

sudo -E LD_LIBRARY_PATH=/home/mxd/gcc/lib64:/home/mxd/lib/dpdk/lib/x86_64-linux-gnu ./run

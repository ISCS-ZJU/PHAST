#!/bin/bash

rm -rf /mnt/pmem/zhenxin/*

CINCLUDE="-I ./source/"
CDEBUG="-O3"
# CDEBUG="-O0 -g"

# CWARNING="-Wall" # open warning info
CWARNING="-w" #close warning info.

CFLAGS="-lpmemobj -lpthread -mclflushopt -mclwb -mavx512f -mavx512bw"


g++ $CINCLUDE $CDEBUG $CWARNING -o simple_test test/simple_test.cc source/PHAST.cc $CFLAGS


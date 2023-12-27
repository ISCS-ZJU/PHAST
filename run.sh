#!/bin/bash

CINCLUDE="-I ./source/"
CDEBUG="-O3"
# CDEBUG="-O0 -g"

# CWARNING="-Wall" # open warning info
CWARNING="-w" #close warning info.

CFLAGS="-lpmemobj -lpthread -march=native"

g++ $CINCLUDE $CDEBUG $CWARNING -o simple_test test/simple_test.cc source/PHAST.cc $CFLAGS


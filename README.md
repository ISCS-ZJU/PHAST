# PHAST: Hierarchical Concurrent Log-Free Skip List for Persistent Memory

## Introduction

PHAST is a persistent skip list which leverages persistent memory to tackle the memory overhead and boost indexing performance. PHAST proposes a relaxed rwlock-based concurrency control strategy to support writelock-free concurrent insert and lock-free concurrent search.

Please read the following paper for more details:

[Zhenxin Li, Bing Jiao, Shuibing He, Weikuan Yu. PHAST: Hierarchical Concurrent Log-Free Skip List for Persistent Memory. TPDS 2022.](https://ieeexplore.ieee.org/abstract/document/9772399)

## Directories

* source: source files for PHAST.
* test: the test file.

## Running

The code is designed for machines equipped with Intel Optane DCPMMs.

Please change the NVM file path `PMEM_PATH` in `source/PHAST.h` before you run experiments.

And then execute the script as following:

```
sh run.sh
./simple_test [the number of threads]
```

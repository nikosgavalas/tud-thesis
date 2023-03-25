[![Tests](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml/badge.svg)](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml)

This repo contains the implementation of a key-value store with three types of backend engines:
1. LSM Tree backend with size-tiered compaction (like [Cassandra](https://cassandra.apache.org/_/index.html)).
2. Backend based on [Microsoft's FASTER](https://microsoft.github.io/FASTER/docs/td-research-papers/).
3. Backend based on an append-only log with in-memory hash-based indexing (similar to [Bitcask](https://riak.com/assets/bitcask-intro.pdf)).

The goal is to compare the performances and determine for which use-cases each type is more suitable, in the context of incremental state management for distributed stream processing.

#### Requirements:

Python 3.11, `pip install -r requirements.txt`

#### Usage:

`./kvstore`

#### Tests:

`python -m unittest`

#### Benchmarks:

`python benchmarks/run.py`

#### Roadmap:

- [x] LSMTree
  - [x] memtable
  - [x] bloom filters
  - [x] flush the memtable
  - [x] file format
    - [x] data
    - [x] fence pointers persistence
    - [x] bloom filters persistence
  - [x] merging multiple levels
  - [x] WAL
  - [x] asyncio (async branch for now)
- [x] HybridLog
  - [x] basis
  - [x] hash index
  - [x] logical addresses
  - [x] compaction
  - [x] asyncio (async branch)
- [x] AppendLog
  - [x] basis
  - [x] compaction
  - [x] merging multiple levels
  - [x] asyncio (async branch)
- [x] Benchmarks
  - [x] distributions
  - [x] basic throughput


#### TODOs
- AppendLog looks abysmal, profiling shows it's mostly because of the fopens. i should keep the files open.

#### Some basic initial measurements

```
number of distinct choices for keys and values: 100
distribution:   Zipfian
1.0e+06 writes  LSMTree:        0.741s
1.0e+06 reads   LSMTree:        0.153s
1.0e+06 writes  HybridLog:      0.374s
1.0e+06 reads   HybridLog:      0.204s
1.0e+06 writes  AppendLog:      2.116s
1.0e+06 reads   AppendLog:      7.609s
distribution:   HotSet
1.0e+06 writes  LSMTree:        0.759s
1.0e+06 reads   LSMTree:        0.158s
1.0e+06 writes  HybridLog:      0.376s
1.0e+06 reads   HybridLog:      0.236s
1.0e+06 writes  AppendLog:      2.209s
1.0e+06 reads   AppendLog:      7.524s
distribution:   Uniform
1.0e+06 writes  LSMTree:        0.768s
1.0e+06 reads   LSMTree:        0.146s
1.0e+06 writes  HybridLog:      0.364s
1.0e+06 reads   HybridLog:      0.212s
1.0e+06 writes  AppendLog:      2.184s
1.0e+06 reads   AppendLog:      7.601s

number of distinct choices for keys and values: 10000
distribution:   Zipfian
1.0e+06 writes  LSMTree:        0.930s
1.0e+06 reads   LSMTree:        0.253s
1.0e+06 writes  HybridLog:      0.785s
1.0e+06 reads   HybridLog:      0.871s
1.0e+06 writes  AppendLog:      2.230s
1.0e+06 reads   AppendLog:      7.620s
distribution:   HotSet
1.0e+06 writes  LSMTree:        0.987s
1.0e+06 reads   LSMTree:        1.291s
1.0e+06 writes  HybridLog:      1.330s
1.0e+06 reads   HybridLog:      2.377s
1.0e+06 writes  AppendLog:      2.371s
1.0e+06 reads   AppendLog:      7.703s
distribution:   Uniform
1.0e+06 writes  LSMTree:        0.939s
1.0e+06 reads   LSMTree:        0.204s
1.0e+06 writes  HybridLog:      1.283s
1.0e+06 reads   HybridLog:      3.584s
1.0e+06 writes  AppendLog:      2.434s
1.0e+06 reads   AppendLog:      7.761s

number of distinct choices for keys and values: 1000000
distribution:   Zipfian
1.0e+06 writes  LSMTree:        2.488s
1.0e+06 reads   LSMTree:        3.474s
1.0e+06 writes  HybridLog:      1.172s
1.0e+06 reads   HybridLog:      2.539s
1.0e+06 writes  AppendLog:      2.569s
1.0e+06 reads   AppendLog:      8.052s
distribution:   HotSet
1.0e+06 writes  LSMTree:        5.182s
1.0e+06 reads   LSMTree:        7.186s
1.0e+06 writes  HybridLog:      1.985s
1.0e+06 reads   HybridLog:      4.844s
1.0e+06 writes  AppendLog:      3.266s
1.0e+06 reads   AppendLog:      8.366s
distribution:   Uniform
1.0e+06 writes  LSMTree:        5.611s
1.0e+06 reads   LSMTree:        10.555s
1.0e+06 writes  HybridLog:      1.793s
1.0e+06 reads   HybridLog:      5.179s
1.0e+06 writes  AppendLog:      3.290s
1.0e+06 reads   AppendLog:      8.199s
```
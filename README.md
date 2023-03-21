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
- AppendLog looks abysmal, I think it is because I don't keep the log open all the time. I should change it asap.
- Do some profiling
- More metrics (like total disk usage?) and more scenarios (instead of just x reads and x writes?)

#### Some basic initial measurements

number of distinct choices for keys and values: 100
distribution:   Zipfian
1.0e+06 writes  LSMTree:        0.770s
1.0e+06 reads   LSMTree:        0.157s
1.0e+06 writes  HybridLog:      0.377s
1.0e+06 reads   HybridLog:      0.201s
1.0e+06 writes  AppendLog:      9.787s
1.0e+06 reads   AppendLog:      7.563s
distribution:   HotSet
1.0e+06 writes  LSMTree:        0.786s
1.0e+06 reads   LSMTree:        0.159s
1.0e+06 writes  HybridLog:      0.370s
1.0e+06 reads   HybridLog:      0.232s
1.0e+06 writes  AppendLog:      9.724s
1.0e+06 reads   AppendLog:      7.669s
distribution:   Uniform
1.0e+06 writes  LSMTree:        0.778s
1.0e+06 reads   LSMTree:        0.156s
1.0e+06 writes  HybridLog:      0.378s
1.0e+06 reads   HybridLog:      0.203s
1.0e+06 writes  AppendLog:      9.836s
1.0e+06 reads   AppendLog:      7.961s

number of distinct choices for keys and values: 10000
distribution:   Zipfian
1.0e+06 writes  LSMTree:        0.990s
1.0e+06 reads   LSMTree:        0.278s
1.0e+06 writes  HybridLog:      1.202s
1.0e+06 reads   HybridLog:      0.906s
1.0e+06 writes  AppendLog:      10.970s
1.0e+06 reads   AppendLog:      8.006s
distribution:   HotSet
1.0e+06 writes  LSMTree:        1.070s
1.0e+06 reads   LSMTree:        1.385s
1.0e+06 writes  HybridLog:      2.335s
1.0e+06 reads   HybridLog:      2.488s
1.0e+06 writes  AppendLog:      10.249s
1.0e+06 reads   AppendLog:      8.296s
distribution:   Uniform
1.0e+06 writes  LSMTree:        1.032s
1.0e+06 reads   LSMTree:        0.191s
1.0e+06 writes  HybridLog:      2.403s
1.0e+06 reads   HybridLog:      3.754s
1.0e+06 writes  AppendLog:      10.250s
1.0e+06 reads   AppendLog:      8.250s

number of distinct choices for keys and values: 1000000
distribution:   Zipfian
1.0e+06 writes  LSMTree:        2.557s
1.0e+06 reads   LSMTree:        3.589s
1.0e+06 writes  HybridLog:      1.927s
1.0e+06 reads   HybridLog:      2.637s
1.0e+06 writes  AppendLog:      10.914s
1.0e+06 reads   AppendLog:      8.518s
distribution:   HotSet
1.0e+06 writes  LSMTree:        5.164s
1.0e+06 reads   LSMTree:        7.131s
1.0e+06 writes  HybridLog:      3.021s
1.0e+06 reads   HybridLog:      5.045s
1.0e+06 writes  AppendLog:      11.544s
1.0e+06 reads   AppendLog:      8.918s
distribution:   Uniform
1.0e+06 writes  LSMTree:        6.270s
1.0e+06 reads   LSMTree:        11.335s
1.0e+06 writes  HybridLog:      3.065s
1.0e+06 reads   HybridLog:      5.237s
1.0e+06 writes  AppendLog:      11.925s
1.0e+06 reads   AppendLog:      8.925s

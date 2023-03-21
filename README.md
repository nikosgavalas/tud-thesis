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

`cd benchmarks && python run.py`

#### Roadmap:

- [ ] LSMTree
  - [x] memtable
  - [x] bloom filters
  - [x] flush the memtable
  - [x] file format
    - [x] data
    - [x] fence pointers persistence
    - [x] bloom filters persistence
  - [x] merging multiple levels
  - [x] WAL
  - [x] asyncio
- [ ] HybridLog
  - [x] basis
  - [x] hash index
  - [x] logical addresses
  - [x] compaction
  - [x] asyncio
- [ ] AppendLog
  - [x] basis
  - [x] compaction
  - [x] merging multiple levels
  - [x] asyncio
- [ ] Benchmarks
  - [x] distributions
  - [ ] basic throughput

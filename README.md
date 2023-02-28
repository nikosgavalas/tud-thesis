[![Tests](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml/badge.svg)](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml)

This repo contains the implementation of a key-value store with three types of backend engines, each based on:
1. LSM Tree with size-tiered compaction.
2. [Microsoft's FASTER](https://microsoft.github.io/FASTER/docs/td-research-papers/).
3. Append-only log with in-memory hash-based indexing.
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
  - [ ] asyncio
- [ ] IndexedLog
  - [x] basis
  - [ ] hash index
  - [x] logical addresses
  - [ ] compaction of segments of the log
  - [ ] merging of multiple segments
  - [ ] asyncio
- [ ] SimpleLog
  - [x] basis
  - [x] compaction
  - [x] merging multiple levels
  - [ ] asyncio
- [ ] Benchmarks

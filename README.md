[![Tests](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml/badge.svg)](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml)

This repo contains the implementations of two types of key-value stores:
1. KV store based on an LSM Tree with size-tiered compaction.
2. KV store based on [Microsoft's FASTER](https://microsoft.github.io/FASTER/docs/td-research-papers/).

The goal is to compare the performances and determine for which use-cases each type is more suitable, in the context of state management in distributed stream processing.

#### Requirements:

Python 3.11, `pip install -r requirements.txt`

#### Usage:

`./kvstore`. AT the moment this will only run the LSM store.

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
  - [ ] logical addresses
  - [ ] compaction of segments of the log
  - [ ] merging of multiple segments
  - [ ] asyncio

[![Tests](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml/badge.svg)](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml)

Key-Value Store based on an LSM Tree with size-tiered compaction.

#### Requirements:

Python 3.11, `pip install -r requirements.txt`

#### Usage:

`./kvstore`

#### Tests:

`python -m unittest`

#### Roadmap:

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

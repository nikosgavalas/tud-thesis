[![Tests](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml/badge.svg)](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml)

Key-value store with three types of backend engines:
1. LSM Tree backend with size-tiered compaction (like [Cassandra](https://cassandra.apache.org/_/index.html)).
2. Backend based on [Microsoft's FASTER](https://microsoft.github.io/FASTER/docs/td-research-papers/).
3. Backend based on an append-only log with in-memory hash-based indexing (similar to [Bitcask](https://riak.com/assets/bitcask-intro.pdf)).

The goal is to compare the performances and determine for which use-cases each
type is more suitable, specifically in the context of incremental snapshotting
and state management in distributed stream processing.

#### Requirements:

See [pyproject.toml](./pyproject.toml).

#### Usage:

`pip install .` and `kevo`

#### Tests:

`python -m unittest discover -s tests`

#### Benchmarks:

`python benchmarks/run.py`

#### TODOs
- asyncio and concurrency control

<!-- docker run --rm --name minio -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minio99" -e "MINIO_ROOT_PASSWORD=minio123" quay.io/minio/minio server /data --console-address ":9001" -->

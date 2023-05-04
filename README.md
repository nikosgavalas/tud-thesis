[![Tests](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml/badge.svg)](https://github.com/nikosgavalas/kvstore/actions/workflows/run_tests.yml)

### Incremental Snapshotting in Transactional Dataflow SFaaS Systems

This repo contains three key-value store implementations:
1. `LSMTree` with size-tiered compaction, like Apache's [Cassandra](https://cassandra.apache.org/_/index.html).
2. `HybridLog`, based on Microsoft's [FASTER](https://microsoft.github.io/FASTER/docs/td-research-papers/).
3. `AppendLog`, similar to Riak's [Bitcask](https://riak.com/assets/bitcask-intro.pdf).

The goal is to compare the performances and determine for which use-cases each
type is more suitable, specifically in the context of incremental snapshotting
for state management in (transactional) dataflow SFaaS systems.

See a rendered version of my thesis [here](./thesis/main.pdf).

**Requirements**: See [pyproject.toml](./pyproject.toml).

**Usage**: `pip install .` and run `kevo` to try the CLI.

**Tests**: `python -m unittest discover -s tests`

**Benchmarks**: See under the [benchmarks](./benchmarks) folder.

<!-- #### TODOs -->
<!-- asyncio and concurrency control -->
<!-- docker run --rm --name minio -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minio99" -e "MINIO_ROOT_PASSWORD=minio123" quay.io/minio/minio server /data --console-address ":9001" -->

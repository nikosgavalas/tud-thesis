import sys
sys.path.append('..')
from utils import *
from distributions import Uniform, Zipfian, HotSet
from kevo import LSMTree, AppendLog, HybridLog, MemOnly, PathReplica, SimpleReplica

import seaborn as sns
import matplotlib.pyplot as plt


def measure_snapshot_time(db, distro, keys_set, vals_set, n_ops, engine, eng_comb, **args):
    res = []

    rng = Random(1)
    klen = 2
    vlen = 8
    incr = 10_000  # has to match the mem of the engine in order to match the flush freq with the snapshot, to be fair

    for i in range(args['n_boxes']):
        for _ in range(incr):
            db[rng.randbytes(klen)] = rng.randbytes(vlen)
        with Timer() as t:
            db.snapshot()
        res.append({'metric': 'snapshot', 'value': float(t), 'n_boxes': (i+1) * (incr * (klen + vlen)) / 1000000})

    return res

latency = 0#10**(-6)
df = run(
    [2], [8], [1], [1],
    [Uniform], [{'seed': [1]}],
    [LSMTree, HybridLog, AppendLog, MemOnly], [{
        'max_runs_per_level': [5],
        'density_factor': [10],
        'memtable_bytes_limit': [20_000_000],
        'replica': [SimpleReplica('./benchmark_data_' + LSMTree.name, '/tmp/remote', network_latency_per_byte=latency)]
    },
    {
        'max_runs_per_level': [5],
        'mem_segment_len': [2_000_000],
        'ro_lag_interval': [800_000],
        'flush_interval': [100_000],
        'hash_index': ['dict'],
        'compaction_enabled': [False],
        'replica': [SimpleReplica('./benchmark_data_' + HybridLog.name, '/tmp/remote', network_latency_per_byte=latency)]
    },
    {
        'max_runs_per_level': [5],
        'threshold': [20_000_000],
        'replica': [SimpleReplica('./benchmark_data_' + AppendLog.name, '/tmp/remote', network_latency_per_byte=latency)]
    },
    {
        'replica': [SimpleReplica('./benchmark_data_' + MemOnly.name, '/tmp/remote', network_latency_per_byte=latency)]
    }],
    [measure_snapshot_time], {'n_boxes': 10}
)


data = df[['n_boxes', 'metric', 'value', 'engine']]

barplot(data, 'n_boxes', 'value', 'snapshot',
         hue='engine',
         X='State Size (MB)', Y='Snapshot time (s)',
         save=False, show=True)

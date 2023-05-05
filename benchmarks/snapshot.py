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
    start = 10_000_000
    incr = 10_000
    n = 20

    size_so_far = 0
    aggr_time = 0

    for _ in range(incr):
        db[rng.randbytes(klen)] = rng.randbytes(vlen)
    with Timer() as t:
        db.snapshot()
    size_so_far += start + incr * (klen + vlen)
    aggr_time += float(t)
    row = {'metric': 'snapshot', 'value': float(t), 'write_volume': size_so_far / 1000000}
    res.append(row)
    row = {'metric': 'snapshot_aggr', 'value': aggr_time, 'write_volume': size_so_far / 1000000}
    print(db.name, row)
    res.append(row)

    for _ in range(n):
        for _ in range(incr):
            db[rng.randbytes(klen)] = rng.randbytes(vlen)
        with Timer() as t:
            db.snapshot()
        size_so_far += start + incr * (klen + vlen)
        aggr_time += float(t)
        row = {'metric': 'snapshot', 'value': float(t), 'write_volume': size_so_far / 1000000}
        res.append(row)
        row = {'metric': 'snapshot_aggr', 'value': aggr_time, 'write_volume': size_so_far / 1000000}
        print(db.name, row)
        res.append(row)

    return res

latency = 10**(-6)
df = run(
    [2], [8], [1], [1],
    [Uniform], [{'seed': [1]}],
    [LSMTree, HybridLog, AppendLog, MemOnly], [{
        'auto_push': [False],
        'max_runs_per_level': [3],
        'density_factor': [10],
        'memtable_bytes_limit': [1_000_000],
        'replica': [SimpleReplica('./benchmark_data_' + LSMTree.name, '/tmp/remote', network_latency_per_byte=latency)]
    },
    {
        'auto_push': [False],
        'max_runs_per_level': [3],
        'mem_segment_len': [1_500_000],
        'ro_lag_interval': [700_000],
        'flush_interval': [700_000],
        'hash_index': ['dict'],
        'compaction_enabled': [False],
        'replica': [SimpleReplica('./benchmark_data_' + HybridLog.name, '/tmp/remote', network_latency_per_byte=latency)]
    },
    {
        'auto_push': [False],
        'max_runs_per_level': [3],
        'threshold': [1_000_000],
        'replica': [SimpleReplica('./benchmark_data_' + AppendLog.name, '/tmp/remote', network_latency_per_byte=latency)]
    },
    {
        'replica': [SimpleReplica('./benchmark_data_' + MemOnly.name, '/tmp/remote', network_latency_per_byte=latency)]
    }],
    [measure_snapshot_time], {}
)


# data = df[['write_volume', 'metric', 'value', 'engine']]
# data = data[data['metric'].isin(['snapshot'])]
#
# barplot(data, 'write_volume', 'value', 'snapshot',
#          hue='engine',
#          X='Write Volume (MB)', Y='Snapshot time (s)',
#          save=False, show=True)
#


#
# data = df[['write_volume', 'metric', 'value', 'engine']]
# data = data[data['metric'].isin(['snapshot'])]
#
# lineplot(data, 'write_volume', 'value', 'snapshot',
#          hue='engine',
#          X='Write Volume (MB)', Y='Snapshot time (s)',
#          save=False, show=True)
#




# data = df[['write_volume', 'metric', 'value', 'engine']]
# data = data[data['metric'].isin(['snapshot_aggr'])]
#
# lineplot(data, 'write_volume', 'value', 'snapshot_aggr',
#          hue='engine',
#          X='Write Volume (MB)', Y='Snapshot time (s)',
#          save=False, show=True)

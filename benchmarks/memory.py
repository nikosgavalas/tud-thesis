from sys import getsizeof
from utils import *
from distributions import Uniform, Zipfian, HotSet
from kevo import LSMTree, AppendLog, HybridLog, MemOnly, PathReplica


def measure_memory(db, distro, keys_set, vals_set, n_ops, engine, eng_comb, **args):
    res = []

    n = 10
    klen, vlen = 5, 5  # match below

    state_size = 0
    incr = len(keys_set) // n
    for _ in range(n):
        for _ in range(incr):
            db[keys_set[next(distro)]] = vals_set[next(distro)]
        state_size += incr * (klen + vlen)
        res.append({'metric': 'mem', 'value': getsizeof(db)/1000, 'state_size': state_size/1000})

    return res


df = run(
    [5], [5], [100_000], [0],
    [Zipfian], [{'seed': [1]}],
    [LSMTree, HybridLog, AppendLog], [{
        'max_runs_per_level': [10],
        'density_factor': [10],
        'memtable_bytes_limit': [10_000_000],
        'replica': [None]
    },
    {
        'max_runs_per_level': [10],
        'mem_segment_len': [1_000_000],
        'ro_lag_interval': [400_000],
        'flush_interval': [400_000],
        'hash_index': ['dict'],
        'compaction_enabled': [False],
        'replica': [None]
    },
    {
        'max_runs_per_level': [10],
        'threshold': [10_000_000],
        'replica': [None]
    }],
    [measure_memory], {}
)

data = df[['state_size', 'metric', 'value', 'engine']]
data = data[data['metric'].isin(['mem'])]
data['value'] = data['value'] * 1000000

lineplot(data, 'state_size', 'value', 'mem-state-size',
         hue='engine',
         style='metric',
         X='State Size (KB)', Y='Memory Usage (KB)',
         save=False, show=True)

from utils import *
from distributions import Uniform, Zipfian, HotSet
from kevo import LSMTree, AppendLog, HybridLog, MemOnly, PathReplica


def measure_throughput_latency_writes(db, distro, keys_set, vals_set, n_ops, engine, eng_comb, **args):
    res = []

    seconds = args['seconds']
    for writes_per_sec in args['writes_per_sec']:
        keys, vals = gen_keys_vals(distro, keys_set, vals_set, writes_per_sec)

        latencies = []
        for i in range(seconds):
            with Timer() as latency:
                for k, v in zip(keys, vals):
                    db[k] = v
            latency = float(latency)
            avg_latency = latency / len(keys)
            # if latency < 1:
            #     sleep(1 - latency)
            latencies.append(avg_latency)

        res.append({'metric': '50p', 'value': percentile(latencies, 50), 'writes_per_sec': writes_per_sec})
        res.append({'metric': '95p', 'value': percentile(latencies, 95), 'writes_per_sec': writes_per_sec})

    return res


df = run(
    [5], [5], [10_000], [0],
    [Uniform], [{'seed': [1]}],
    [LSMTree, HybridLog, AppendLog], [{
        'max_runs_per_level': [10],
        'density_factor': [10],
        'memtable_bytes_limit': [10_000_000],
        'replica': [None]
    },
    {
        'max_runs_per_level': [10],
        'mem_segment_len': [1_000_000],
        'ro_lag_interval': [100_000],
        'flush_interval': [100_000],
        'hash_index': ['dict'],
        'compaction_enabled': [False],
        'replica': [None]
    },
    {
        'max_runs_per_level': [10],
        'threshold': [10_000_000],
        'replica': [None]
    }],
    [measure_throughput_latency_writes], {'seconds': 10, 'writes_per_sec': list(range(10_000, 100_000, 10_000))}
)

data = df[['writes_per_sec', 'metric', 'value', 'engine']]
data = data[data['metric'].isin(['50p', '95p'])]
data['value'] = data['value'] * 1000000

lineplot(data, 'writes_per_sec', 'value', 'write-throughput',
         hue='engine',
         style='metric',
         X='Throughput (writes/sec)', Y='Latency (us)',
         save=False, show=True)

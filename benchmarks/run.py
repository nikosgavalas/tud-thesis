import sys
import os
from random import Random
import shutil
sys.path.append('.')  # make it runnable from the top level

import pandas as pd
from tqdm import tqdm

from src.lsmtree import LSMTree
from src.hybridlog import HybridLog
from src.appendlog import AppendLog

from benchmarks.distributions import Uniform, Zipfian, HotSet
from benchmarks.timer import Timer


def measure_write_and_size(engine, all_keys, all_vals, data, kvl, n_iter, n_item, distro_name):
    with Timer(truncate=True) as t:
        for k, v in zip(all_keys, all_vals):
            engine[k] = v
    data.append({'kv_len': kvl, 'n_iters': n_iter, 'n_items': n_item, 'distro': distro_name, 'engine': engine.name, 'metric': 'write', 'value': t})
    engine.close()

    dir_size_bytes = sum(os.path.getsize(os.path.join(engine.data_dir, f)) for f in os.listdir(engine.data_dir) if os.path.isfile(os.path.join(engine.data_dir, f)))
    data.append({'kv_len': kvl, 'n_iters': n_iter, 'n_items': n_item, 'distro': distro_name, 'engine': engine.name, 'metric': 'size', 'value': dir_size_bytes})


def measure_read(engine, all_keys, all_vals, data, kvl, n_iter, n_item, distro_name):
    with Timer(truncate=True) as t:
        for k, v in zip(all_keys, all_vals):
            engine[k]
    data.append({'kv_len': kvl, 'n_iters': n_iter, 'n_items': n_item, 'distro': distro_name, 'engine': engine.name, 'metric': 'read', 'value': t})
    engine.close()

    shutil.rmtree(engine.data_dir)


def compare_engines_default_args(seed, rng, base_dirname, n_items, n_iters, key_val_lens):
    data = []

    for kvl in tqdm(key_val_lens, desc='kvl', position=0):
        for n_iter in tqdm(n_iters, desc=' iters', position=1, leave=False):
            for n_item in tqdm(n_items, desc='  items', position=2, leave=False):
                distributions = [Zipfian(items=n_item, seed=seed), HotSet(items=n_item, seed=seed), Uniform(items=n_item, seed=seed)]

                keys = [rng.randbytes(rng.randint(1, kvl)) for _ in range(n_item)]
                vals = [rng.randbytes(rng.randint(0, kvl)) for _ in range(n_item)]

                for distro in tqdm(distributions, desc='   distro', position=3, leave=False):
                    all_keys = [keys[next(distro)] for _ in range(n_iter)]
                    all_vals = [vals[next(distro)] for _ in range(n_iter)]

                    engine = LSMTree(base_dirname + LSMTree.name)
                    measure_write_and_size(engine, all_keys, all_vals, data, kvl, n_iter, n_item, distro.name)
                    with Timer(truncate=True) as t: # measure recovery time
                        engine = LSMTree(base_dirname + LSMTree.name)
                    data.append({'kv_len': kvl, 'n_iters': n_iter, 'n_items': n_item, 'distro': distro.name, 'engine': engine.name, 'metric': 'recovery', 'value': t})
                    measure_read(engine, all_keys, all_vals, data, kvl, n_iter, n_item, distro.name)

                    engine = HybridLog(base_dirname + HybridLog.name, max_key_len=kvl, max_value_len=kvl)
                    measure_write_and_size(engine, all_keys, all_vals, data, kvl, n_iter, n_item, distro.name)
                    with Timer(truncate=True) as t: # measure recovery time
                        engine = HybridLog(base_dirname + HybridLog.name, max_key_len=kvl, max_value_len=kvl)
                    data.append({'kv_len': kvl, 'n_iters': n_iter, 'n_items': n_item, 'distro': distro.name, 'engine': engine.name, 'metric': 'recovery', 'value': t})
                    measure_read(engine, all_keys, all_vals, data, kvl, n_iter, n_item, distro.name)

                    engine = AppendLog(base_dirname + AppendLog.name)
                    measure_write_and_size(engine, all_keys, all_vals, data, kvl, n_iter, n_item, distro.name)
                    with Timer(truncate=True) as t: # measure recovery time
                        engine = AppendLog(base_dirname + AppendLog.name)
                    data.append({'kv_len': kvl, 'n_iters': n_iter, 'n_items': n_item, 'distro': distro.name, 'engine': engine.name, 'metric': 'recovery', 'value': t})
                    measure_read(engine, all_keys, all_vals, data, kvl, n_iter, n_item, distro.name)

    return data


def measure_LSMTree(base_dir, all_keys, all_vals, max_runs_per_levels, density_factors, memtable_bytes_limits):
    data = []
    for max_runs_per_level in tqdm(max_runs_per_levels, desc='max_runs_per_level', position=0):
        for density_factor in tqdm(density_factors, desc=' density_factor', position=1, leave=False):
            for memtable_bytes_limit in tqdm(memtable_bytes_limits, desc='  memtable_bytes_limit', position=2, leave=False):
                db = LSMTree(base_dir + LSMTree.name, max_runs_per_level, density_factor, memtable_bytes_limit)
                with Timer(truncate=True) as t:
                    for k, v in zip(all_keys, all_vals):
                        db[k] = v
                data.append({'max_runs_per_level': max_runs_per_level, 'density_factor': density_factor, 'memtable_bytes_limit': memtable_bytes_limit, 'op': 'write', 'value': t})
                with Timer(truncate=True) as t:
                    for k, v in zip(all_keys, all_vals):
                        db[k]
                data.append({'max_runs_per_level': max_runs_per_level, 'density_factor': density_factor, 'memtable_bytes_limit': memtable_bytes_limit, 'op': 'read', 'value': t})
                db.close()
                shutil.rmtree(base_dir + LSMTree.name)
    return data


def measure_HybridLog(base_dir, all_keys, all_vals, max_kv_lens, mem_segment_lens, ro_lag_intervals, flush_intervals, hash_indices, compaction_intervals):
    data = []
    for max_kv_len in tqdm(max_kv_lens, desc='max_kv_len', position=0):
        for mem_segment_len in tqdm(mem_segment_lens, desc=' mem_segment_len', position=1, leave=False):
            for ro_lag_interval in tqdm(ro_lag_intervals, desc='  ro_lag_interval', position=2, leave=False):
                for flush_interval in tqdm(flush_intervals, desc='   flush_interval', position=3, leave=False):
                    for hash_index in tqdm(hash_indices, desc='    hash_index', position=4, leave=False):
                        for compaction_interval in tqdm(compaction_intervals, desc='     compaction_interval', position=5, leave=False):
                            db = HybridLog(base_dir + HybridLog.name, max_kv_len, max_kv_len, mem_segment_len, ro_lag_interval, flush_interval, hash_index, compaction_interval)
                            with Timer(truncate=True) as t:
                                for k, v in zip(all_keys, all_vals):
                                    db[k] = v
                            data.append({'max_kv_len': max_kv_len, 'mem_segment_len': mem_segment_len, 'ro_lag_interval': ro_lag_interval, 'flush_interval': flush_interval, 'hash_index': hash_index, 'compaction_interval': compaction_interval, 'op': 'write', 'value': t})
                            with Timer(truncate=True) as t:
                                for k, v in zip(all_keys, all_vals):
                                    db[k]
                            data.append({'max_kv_len': max_kv_len, 'mem_segment_len': mem_segment_len, 'ro_lag_interval': ro_lag_interval, 'flush_interval': flush_interval, 'hash_index': hash_index, 'compaction_interval': compaction_interval, 'op': 'read', 'value': t})
                            db.close()
                            shutil.rmtree(base_dir + HybridLog.name)
    return data


def measure_AppendLog(base_dir, all_keys, all_vals, max_runs_per_levels, thresholds):
    data = []
    for max_runs_per_level in tqdm(max_runs_per_levels, desc='max_runs_per_level', position=0):
        for threshold in tqdm(thresholds, desc=' threshold', position=1, leave=False):
            db = AppendLog(base_dir + AppendLog.name, max_runs_per_level, threshold=threshold)
            with Timer(truncate=True) as t:
                for k, v in zip(all_keys, all_vals):
                    db[k] = v
            data.append({'max_runs_per_level': max_runs_per_level, 'threshold': threshold, 'op': 'write', 'value': t})
            with Timer(truncate=True) as t:
                for k, v in zip(all_keys, all_vals):
                    db[k]
            data.append({'max_runs_per_level': max_runs_per_level, 'threshold': threshold, 'op': 'read', 'value': t})
            db.close()
            shutil.rmtree(base_dir + AppendLog.name)
    return data


def get_kvs(rng, n_items, n_iters, distro, kvlen):
    keys = [rng.randbytes(rng.randint(1, kvlen)) for _ in range(n_items)]
    vals = [rng.randbytes(rng.randint(0, kvlen)) for _ in range(n_items)]
    all_keys = [keys[next(distro)] for _ in range(n_iters)]
    all_vals = [vals[next(distro)] for _ in range(n_iters)]
    return all_keys, all_vals


def main():
    seed = 1
    rng = Random(seed)
    base_dir = './benchmark_data_'

    n_items = 10_000
    n_iters = 1_000_000
    distro = Uniform(items=n_items, seed=seed)
    kvlen = 4

    all_keys, all_vals = get_kvs(rng, n_items, n_iters, distro, kvlen)

    print('starting lsm...')
    data = measure_LSMTree(base_dir, all_keys, all_vals, [1, 10], [10, 100, 1_000], [100_000, 1_000_000, 10_000_000])
    lsm_df = pd.DataFrame.from_dict(data)
    lsm_df.to_csv('lsm.csv', index=None)
    print('lsm done.')

    print('starting hlog...')
    data = measure_HybridLog(base_dir, all_keys, all_vals, [4, 8], [2 ** 20], [2 ** 10], [4 * 2 ** 10], ['dict'], [0, 10, 100])
    hlog_df = pd.DataFrame.from_dict(data)
    hlog_df.to_csv('hlog.csv', index=None)
    print('hlog done.')

    print('starting alog...')
    data = measure_AppendLog(base_dir, all_keys, all_vals, [1, 10], [1_000_000, 10_000_000])
    alog_df = pd.DataFrame.from_dict(data)
    alog_df.to_csv('alog.csv', index=None)
    print('alog done.')

    print('starting engines...')
    data = compare_engines_default_args(seed, rng, base_dir, [10, 1_000, 100_000], [100_000, 1_000_000], [4, 8])
    engines_df = pd.DataFrame.from_dict(data)
    engines_df.to_csv('engines.csv', index=None)
    print('engines done.')


if __name__ == '__main__':
    main()

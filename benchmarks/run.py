import itertools
import os
import shutil
from random import Random

import pandas as pd
from tqdm import tqdm

from benchmarks import Timer, Uniform, Zipfian, HotSet
from kevo import LSMTree, HybridLog, AppendLog


def explode(d):
    return (dict(zip(d, v)) for v in itertools.product(*d.values()))


def main():
    seed = 1
    rng = Random(seed)
    base_dir = './benchmark_data_'

    data = []

    kvlens = [4, 8]
    n_items_list = [100, 10_000]
    n_ops_list = [1_000, 10_000]
    distros = [Uniform, Zipfian, HotSet]
    distros_args = [
        # 'items' overwritten later because they need to match the n_items.
        {'items': None, 'seed': [seed]},
        {'items': None, 'seed': [seed]},
        {'items': None, 'n_sets': [5], 'rotation_interval': [100], 'seed': [seed]}
    ]

    engines = [LSMTree, HybridLog, AppendLog]
    engines_args = [
        {
            'data_dir': [base_dir + LSMTree.name],
            'max_runs_per_level': [1, 3, 5],
            'density_factor': [1, 50, 100],
            'memtable_bytes_limit': [10_000, 1_000_000],
            'replica': [None]
        },
        {
            # 'max_key_len' and 'max_value_len' overwritten later cause they need to match kvlen
            'data_dir': [base_dir + HybridLog.name],
            'max_key_len': None,
            'max_value_len': None,
            'mem_segment_len': [2 ** 20],
            'ro_lag_interval': [2 ** 10],
            'flush_interval': [4 * 2 ** 10],
            'hash_index': ['dict'],
            'compaction_interval': [0],
            'replica': [None]
        },
        {
            'data_dir': [base_dir + AppendLog.name],
            'max_runs_per_level': [1, 3, 5],
            'threshold': [1_000_000, 4_000_000],
            'replica': [None]
        }
    ]

    for kvlen, n_items, n_ops in tqdm(list(itertools.product(kvlens, n_items_list, n_ops_list))):
        for distro_cl, distro_args in zip(distros, distros_args):
            # overwrite distro args
            distro_args['items'] = [n_items]
            for distro_comb in explode(distro_args):
                distro = distro_cl(**distro_comb)

                keys_set = [rng.randbytes(rng.randint(1, kvlen)) for _ in range(n_items)]
                vals_set = [rng.randbytes(rng.randint(0, kvlen)) for _ in range(n_items)]
                keys = [keys_set[next(distro)] for _ in range(n_ops)]
                vals = [vals_set[next(distro)] for _ in range(n_ops)]

                for engine, engine_args in zip(engines, engines_args):
                    # overwrite distro args
                    if 'max_key_len' in engine_args:
                        engine_args['max_key_len'] = [kvlen]
                    if 'max_value_len' in engine_args:
                        engine_args['max_value_len'] = [kvlen]
                    for eng_comb in explode(engine_args):
                        db = engine(**eng_comb)

                        # measure writes
                        with Timer() as t:
                            for k, v in zip(keys, vals):
                                db[k] = v
                        data.append({'kvlen': kvlen, 'n_ops': n_ops, 'n_items': n_items, 'distro': distro.name,
                                     **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'write', 'value': t})

                        # measure data directory size
                        data_dir = eng_comb['data_dir']
                        db.close()
                        dir_size_bytes = sum(
                            os.path.getsize(os.path.join(data_dir, f)) for f in os.listdir(data_dir) if
                            os.path.isfile(os.path.join(data_dir, f)))
                        data.append(
                            {'kvlen': kvlen, 'n_ops': n_ops, 'n_items': n_items, 'distro': distro.name, **distro_comb,
                             'engine': engine.name, **eng_comb, 'metric': 'size', 'value': dir_size_bytes})

                        # measure recovery time
                        with Timer() as t:
                            db = engine(**eng_comb)
                        data.append(
                            {'kvlen': kvlen, 'n_ops': n_ops, 'n_items': n_items, 'distro': distro.name, **distro_comb,
                             'engine': engine.name, **eng_comb, 'metric': 'recovery', 'value': t})

                        # measure reads
                        with Timer() as t:
                            for k, v in zip(keys, vals):
                                _ = db[k]
                        data.append({'kvlen': kvlen, 'n_ops': n_ops, 'n_items': n_items, 'distro': distro.name,
                                     **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'read', 'value': t})

                        db.close()
                        shutil.rmtree(eng_comb['data_dir'])

    pd.DataFrame.from_dict(data).to_csv('measurements.csv', index=None)


if __name__ == '__main__':
    main()

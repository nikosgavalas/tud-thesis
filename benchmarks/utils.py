import sys
from sys import getsizeof
import itertools
import os
import shutil
from random import Random

import pandas as pd
import psutil
from tqdm import tqdm

sys.path.append('.')  # make it runnable from the top level

from benchmarks import Timer, Uniform, Zipfian, HotSet, make_distros_args, make_engines_args, make_experiments_args
from kevo import LSMTree, HybridLog, AppendLog, PathReplica


def explode(d):
    return (dict(zip(d, v)) for v in itertools.product(*d.values()))


def get_dir_size_bytes(d):
    return sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d) if os.path.isfile(os.path.join(d, f)))


def run(klens, vlens, n_items_list, n_ops_list,
        distros, distros_args,
        engines, engines_args,
        times=1, seed=1, base_dir='./benchmark_data_',
        measure_cpu=False, measure_mem=False, measure_write=False, measure_read=False,
        measure_disk_local=False, measure_disk_remote=False, measure_recovery=False,
        return_df=False, show_progress=False):
    rng = Random(seed)
    data = []
    for _ in range(times):
        for klen, vlen, n_items, n_ops in tqdm(list(itertools.product(klens, vlens, n_items_list, n_ops_list)),
                                               desc='Global', position=0, disable=not show_progress):
            for distro_cl, distro_args in tqdm(zip(distros, distros_args), desc=' Distros', position=1, leave=False,
                                               disable=not show_progress):
                # overwrite distro args
                distro_args['items'] = [n_items]
                for distro_comb in explode(distro_args):
                    distro = distro_cl(**distro_comb)

                    keys_set = [rng.randbytes(klen) for _ in range(n_items)]
                    vals_set = [rng.randbytes(vlen) for _ in range(n_items)]
                    keys = [keys_set[next(distro)] for _ in range(n_ops)]
                    vals = [vals_set[next(distro)] for _ in range(n_ops)]

                    for engine, engine_args in tqdm(zip(engines, engines_args), desc='  Engines', position=2,
                                                    leave=False,
                                                    disable=not show_progress):
                        # overwrite distro args
                        engine_args['data_dir'] = [base_dir + engine.name]
                        engine_args['max_key_len'] = [klen]
                        engine_args['max_value_len'] = [vlen]
                        for eng_comb in explode(engine_args):
                            db = engine(**eng_comb)
                            data_dir = eng_comb['data_dir']

                            if type(eng_comb['replica']) is PathReplica and measure_disk_remote:
                                remote_dir = eng_comb['replica'].remote_dir_path

                            if measure_cpu:
                                psutil.cpu_percent()

                            if measure_write:
                                with Timer() as t:
                                    for k, v in zip(keys, vals):
                                        db[k] = v
                                data.append(
                                    {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                     'distro': distro.name,
                                     **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'write', 'value': t})
                                data.append(
                                    {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                     'distro': distro.name,
                                     **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'write_t',
                                     'value': 1 / float(t)})

                            if measure_mem:
                                mem = getsizeof(db)
                                data.append(
                                    {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                     'distro': distro.name,
                                     **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'mem', 'value': mem})

                            db.close()

                            if measure_disk_local:
                                dir_size_bytes = get_dir_size_bytes(data_dir)
                                data.append(
                                    {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                     'distro': distro.name,
                                     **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'disk_local',
                                     'value': dir_size_bytes})

                            if type(eng_comb['replica']) is PathReplica and measure_disk_remote:
                                dir_size_bytes = get_dir_size_bytes(remote_dir)
                                data.append(
                                    {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                     'distro': distro.name,
                                     **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'disk_remote',
                                     'value': dir_size_bytes})

                            with Timer() as t:
                                db = engine(**eng_comb)

                            # recovery local/remote depends on whether 'replica' is given in engine constructor arguments.
                            if measure_recovery:
                                data.append(
                                    {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                     'distro': distro.name,
                                     **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'recovery',
                                     'value': t})

                            if measure_read:
                                with Timer() as t:
                                    for k in keys:
                                        _ = db[k]
                                data.append(
                                    {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                     'distro': distro.name,
                                     **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'read', 'value': t})
                                data.append(
                                    {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                     'distro': distro.name,
                                     **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'read_t',
                                     'value': 1 / float(t)})

                            db.close()

                            if measure_cpu:
                                cpu = psutil.cpu_percent()
                                data.append(
                                    {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                     'distro': distro.name,
                                     **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'cpu', 'value': cpu})

                            del db
                            shutil.rmtree(data_dir)
                            if type(eng_comb['replica']) is PathReplica and measure_disk_remote:
                                eng_comb['replica'].destroy()

    if return_df:
        df = pd.DataFrame.from_dict(data)
        df['replica'] = df['replica'].apply(lambda x: x is not None)
        df['value'] = df['value'].astype(float)
        df['distro'] = df['distro'].astype(str)
        df['engine'] = df['engine'].astype(str)
        return df
    return data


def run2(klens, vlens, n_items_list, n_ops_list,
         distros, distros_args,
         engines, engines_args,
         times=1, increments=10, seed=1, base_dir='./benchmark_data_',
         measure_write=False, measure_disk_local=False, measure_disk_remote=False,
         return_df=False, show_progress=False):
    rng = Random(seed)
    data = []
    for _ in range(times):
        for klen, vlen, n_items, n_ops in tqdm(list(itertools.product(klens, vlens, n_items_list, n_ops_list)),
                                               desc='Global', position=0, disable=not show_progress):
            for distro_cl, distro_args in tqdm(zip(distros, distros_args), desc=' Distros', position=1, leave=False,
                                               disable=not show_progress):
                # overwrite distro args
                distro_args['items'] = [n_items]
                for distro_comb in explode(distro_args):
                    distro = distro_cl(**distro_comb)

                    keys_set = [rng.randbytes(klen) for _ in range(n_items)]
                    vals_set = [rng.randbytes(vlen) for _ in range(n_items)]
                    keys = [keys_set[next(distro)] for _ in range(n_ops)]
                    vals = [vals_set[next(distro)] for _ in range(n_ops)]

                    for engine, engine_args in tqdm(zip(engines, engines_args), desc='  Engines', position=2,
                                                    leave=False,
                                                    disable=not show_progress):
                        # overwrite distro args
                        engine_args['data_dir'] = [base_dir + engine.name]
                        engine_args['max_key_len'] = [klen]
                        engine_args['max_value_len'] = [vlen]
                        for eng_comb in explode(engine_args):
                            db = engine(**eng_comb)
                            data_dir = eng_comb['data_dir']

                            if type(eng_comb['replica']) is PathReplica and measure_disk_remote:
                                remote_dir = eng_comb['replica'].remote_dir_path

                            for inc in range(increments):
                                if measure_write:
                                    with Timer() as t:
                                        for k, v in zip(keys, vals):
                                            db[k] = v
                                    data.append(
                                        {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                         'distro': distro.name, 'inc': inc,
                                         **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'write', 'value': t})
                                    data.append(
                                        {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                         'distro': distro.name, 'inc': inc,
                                         **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'write_t',
                                         'value': 1 / float(t)})

                                if measure_disk_local:
                                    dir_size_bytes = get_dir_size_bytes(data_dir)
                                    data.append(
                                        {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                         'distro': distro.name, 'inc': inc,
                                         **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'disk_local',
                                         'value': dir_size_bytes})

                                if type(eng_comb['replica']) is PathReplica and measure_disk_remote:
                                    dir_size_bytes = get_dir_size_bytes(remote_dir)
                                    data.append(
                                        {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                         'distro': distro.name, 'inc': inc,
                                         **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'disk_remote',
                                         'value': dir_size_bytes})

                            db.close()

                            del db
                            shutil.rmtree(data_dir)
                            if type(eng_comb['replica']) is PathReplica and measure_disk_remote:
                                eng_comb['replica'].destroy()

    if return_df:
        df = pd.DataFrame.from_dict(data)
        df['replica'] = df['replica'].apply(lambda x: x is not None)
        df['value'] = df['value'].astype(float)
        df['distro'] = df['distro'].astype(str)
        df['engine'] = df['engine'].astype(str)
        return df
    return data


def run_experiment(experiment_id, seed=1, base_dir='./benchmark_data_', outfile='measurements.csv'):
    klens, vlens, n_items_list, n_ops_list = make_experiments_args(experiment_id)

    distros = [Uniform, Zipfian, HotSet]
    distros_args = make_distros_args(experiment_id, seed)
    engines = [LSMTree, HybridLog, AppendLog]
    engines_args = make_engines_args(experiment_id, base_dir)

    data = run(klens, vlens, n_items_list, n_ops_list, distros, distros_args, engines,
               engines_args, base_dir=base_dir, seed=seed)

    pd.DataFrame.from_dict(data).to_csv(outfile, index=None)


if __name__ == '__main__':
    run_experiment(1)

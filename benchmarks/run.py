import sys
from sys import getsizeof
import itertools
import os
import shutil
from random import Random

import pandas as pd
import psutil
from tqdm import tqdm

from benchmarks import Timer, Uniform, Zipfian, HotSet, make_distros_args, make_engines_args, make_experiments_args
from kevo import LSMTree, HybridLog, AppendLog

sys.path.append('.')  # make it runnable from the top level


def explode(d):
    return (dict(zip(d, v)) for v in itertools.product(*d.values()))


def main():
    experiment_id = 0

    seed = 1
    base_dir = './benchmark_data_'
    outfile = 'measurements.csv'

    rng = Random(seed)
    data = []

    klens, vlens, n_items_list, n_ops_list = make_experiments_args(experiment_id)

    distros = [Uniform, Zipfian, HotSet]
    distros_args = make_distros_args(experiment_id, seed)
    engines = [LSMTree, HybridLog, AppendLog]
    engines_args = make_engines_args(experiment_id, base_dir)

    for klen, vlen, n_items, n_ops in tqdm(list(itertools.product(klens, vlens, n_items_list, n_ops_list)),
                                           desc='Global', position=0):
        for distro_cl, distro_args in tqdm(zip(distros, distros_args), desc=' Distros', position=1, leave=False):
            # overwrite distro args
            distro_args['items'] = [n_items]
            for distro_comb in explode(distro_args):
                distro = distro_cl(**distro_comb)

                keys_set = [rng.randbytes(klen) for _ in range(n_items)]
                vals_set = [rng.randbytes(vlen) for _ in range(n_items)]
                keys = [keys_set[next(distro)] for _ in range(n_ops)]
                vals = [vals_set[next(distro)] for _ in range(n_ops)]

                for engine, engine_args in tqdm(zip(engines, engines_args), desc='  Engines', position=2, leave=False):
                    # overwrite distro args
                    engine_args['max_key_len'] = [klen]
                    engine_args['max_value_len'] = [vlen]
                    for eng_comb in explode(engine_args):
                        db = engine(**eng_comb)
                        data_dir = eng_comb['data_dir']

                        # measure cpu - start
                        psutil.cpu_percent()

                        # measure writes
                        with Timer() as t:
                            for k, v in zip(keys, vals):
                                db[k] = v
                        data.append(
                            {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items, 'distro': distro.name,
                             **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'write', 'value': t})

                        # measure mem usage
                        mem = getsizeof(db)
                        data.append(
                            {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items, 'distro': distro.name,
                             **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'ram', 'value': mem})

                        db.close()

                        # measure data directory size
                        dir_size_bytes = sum(
                            os.path.getsize(os.path.join(data_dir, f)) for f in os.listdir(data_dir) if
                            os.path.isfile(os.path.join(data_dir, f)))
                        data.append(
                            {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items, 'distro': distro.name,
                             **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'size',
                             'value': dir_size_bytes})

                        # measure recovery time
                        with Timer() as t:
                            db = engine(**eng_comb)
                        data.append(
                            {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items, 'distro': distro.name,
                             **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'recovery', 'value': t})

                        # measure reads
                        with Timer() as t:
                            for k, v in zip(keys, vals):
                                _ = db[k]
                        data.append(
                            {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items, 'distro': distro.name,
                             **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'read', 'value': t})

                        db.close()

                        # measure cpu - end
                        cpu = psutil.cpu_percent()
                        data.append(
                            {'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items, 'distro': distro.name,
                             **distro_comb, 'engine': engine.name, **eng_comb, 'metric': 'cpu', 'value': cpu})

                        del db
                        shutil.rmtree(eng_comb['data_dir'])

    pd.DataFrame.from_dict(data).to_csv(outfile, index=None)


if __name__ == '__main__':
    main()

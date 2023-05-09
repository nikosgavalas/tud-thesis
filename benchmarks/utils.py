import sys
from sys import getsizeof
import itertools
import os
import shutil
from random import Random
from time import time, sleep
import matplotlib.pyplot as plt

import pandas as pd
from numpy import percentile
import seaborn as sns
from tqdm import tqdm

sys.path.append('.')  # make it runnable from the top level

from benchmarks import Timer, Uniform, Zipfian, HotSet
from kevo import PathReplica


figures_dir = '../thesis/Figures'


def explode(d):
    return (dict(zip(d, v)) for v in itertools.product(*d.values()))


def get_dir_size_bytes(d):
    return sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d) if os.path.isfile(os.path.join(d, f)))


def measure_writes(db, distro, keys_set, vals_set, n_ops, engine, eng_comb, **args):
    keys, vals = gen_keys_vals(distro, keys_set, vals_set, n_ops)
    with Timer() as t:
        for k, v in zip(keys, vals):
            db[k] = v
    return [{'metric': 'writes', 'value': float(t)}]


def measure_reads(db, distro, keys_set, vals_set, n_ops, engine, eng_comb, **args):
    keys, _ = gen_keys_vals(distro, keys_set, vals_set, n_ops)
    with Timer() as t:
        for k in keys:
            _ = db[k]
    return [{'metric': 'reads', 'value': float(t)}]


def measure_mem(db, distro, keys_set, vals_set, n_ops, engine, eng_comb, **args):
    return [{'metric': 'mem', 'value': getsizeof(db)}]


def measure_snapshot(db, distro, keys_set, vals_set, n_ops, engine, eng_comb, **args):
    with Timer() as t:
        db.snapshot()
    return [{'metric': 'snapshot', 'value': float(t)}]


def measure_disk_local(db, distro, keys_set, vals_set, n_ops, engine, eng_comb, **args):
    return [{'metric': 'disk_local', 'value': get_dir_size_bytes(db.data_dir)}]


def measure_disk_remote(db, distro, keys_set, vals_set, n_ops, engine, eng_comb, **args):
    return [{'metric': 'disk_remote', 'value': get_dir_size_bytes(db.replica.remote_dir_path)}]


def measure_recovery(db, distro, keys_set, vals_set, n_ops, engine, eng_comb, **args):
    db.close()
    with Timer() as t:
        db = engine(**eng_comb)
    return [{'metric': 'recovery', 'value': float(t)}]


def gen_keys_vals(distro, keys_set, vals_set, n_ops):
    return [keys_set[next(distro)] for _ in range(n_ops)], [vals_set[next(distro)] for _ in range(n_ops)]


def run(klens, vlens, n_items_list, n_ops_list,
             distros, distros_args,
             engines, engines_args,
             funcs, args,
             seed=1, base_dir='./benchmark_data_',
             show_progress=False):
    rng = Random(seed)
    data = []
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

                for engine, engine_args in tqdm(zip(engines, engines_args), desc='  Engines', position=2,
                                                leave=False,
                                                disable=not show_progress):
                    # overwrite distro args
                    engine_args['data_dir'] = [base_dir + engine.name]
                    engine_args['max_key_len'] = [klen]
                    engine_args['max_value_len'] = [vlen]
                    for eng_comb in explode(engine_args):
                        # instantiate
                        db = engine(**eng_comb)
                        data_dir = eng_comb['data_dir']
                        ###############################

                        for func in funcs:
                            rows = func(db, distro, keys_set, vals_set, n_ops, engine, eng_comb, **args)
                            for row in rows:
                                data.append({
                                    'klen': klen, 'vlen': vlen, 'n_ops': n_ops, 'n_items': n_items,
                                    'distro': distro.name, **distro_comb,
                                    'engine': engine.name, **eng_comb,
                                    **row
                                })

                        ###############################
                        # cleanup
                        db.close()
                        del db
                        shutil.rmtree(data_dir)
                        if type(eng_comb['replica']) is PathReplica:
                            eng_comb['replica'].destroy()

    df = pd.DataFrame.from_dict(data)
    df['replica'] = df['replica'].apply(lambda x: x is not None)
    df['value'] = df['value'].astype(float)
    df['distro'] = df['distro'].astype(str)
    df['engine'] = df['engine'].astype(str)

    return df


def lineplot(data, x, y, filename,
             hue=None, style=None,
             logx=False, rotatex=False,
             title=None, ylim=None,
             caption=None, X=None, Y=None,
             save=True, show=False):
    filename += '.png'
    plot = sns.lineplot(data=data, x=x, y=y, hue=hue, style=style)
    if title:
        plot.set_title(title)
    if X:
        plot.set_xlabel(X)
    if Y:
        plot.set_ylabel(Y)
    if ylim is not None:
        plot.set(ylim=ylim)
    if logx:
        plot.set(xscale='log')
    if rotatex:
        plot.set_xticklabels(plot.get_xticklabels(), rotation=30)

    fig = plot.get_figure()

    if save:
        fig.savefig(f"{figures_dir}/{filename}")
    if show:
        plt.show()

    if not caption:
        caption = filename

    print(f'''
\\begin{{figure}}[h]
    \centering
    \includegraphics[width=0.25\\textwidth]{{{filename}}}
    \caption{{{caption}}}
    \label{{fig:{filename}}}
\end{{figure}}
''')

def barplot(data, x, y, filename,
             hue=None,
             title=None, ylim=None,
             caption=None, X=None, Y=None,
             save=True, show=False):
    filename += '.png'
    plot = sns.barplot(data=data, x=x, y=y, hue=hue)
    if title:
        plot.set_title(title)
    if X:
        plot.set_xlabel(X)
    if Y:
        plot.set_ylabel(Y)
    if ylim is not None:
        plot.set(ylim=ylim)

    fig = plot.get_figure()

    if save:
        fig.savefig(f"{figures_dir}/{filename}")
    if show:
        plt.show()

    if not caption:
        caption = filename

    print(f'''
\\begin{{figure}}[h]
    \centering
    \includegraphics[width=0.25\\textwidth]{{{filename}}}
    \caption{{{caption}}}
    \label{{fig:{filename}}}
\end{{figure}}
''')

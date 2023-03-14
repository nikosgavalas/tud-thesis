import sys
from random import Random
sys.path.append('.')  # make it runnable from the top level

from src.lsmtree import LSMTree
from src.hybridlog import HybridLog
from src.appendlog import AppendLog

from distributions import Uniform
from time import Timer


def generate_pairs(rng, key_len, val_len, n_items):
    return (
        [rng.randbytes(rng.randint(1, key_len)) for _ in range(n_items)],
        [rng.randbytes(rng.randint(0, val_len)) for _ in range(n_items)]
    )


def main():
    base_dirname = './benchmark_data_'
    n_items = 100
    n_iter = 1000
    distribution = Uniform(n_items, 1)
    seed = 1

    rng = Random(seed)

    # engine_names = ['LSMTree', 'HybridLog', 'AppendLog']
    # engines = [
    #     LSMTree(base_dirname + 'lsm'),
    #     HybridLog(base_dirname + 'hlog'),
    #     AppendLog(base_dirname + 'alog')
    # ]

    keys, vals = generate_pairs(rng, 4, 4, n_items)

    all_keys = [keys[next(distribution)] for _ in range(n_iter)]
    all_vals = [vals[next(distribution)] for _ in range(n_iter)]

    # TODO: for engine_name, engine in zip(engine_names, engines):
    l = LSMTree('asdf')
    with Timer() as t:
        for k, v in zip(all_keys, all_vals):
            l[k] = v
    print(f'{"LSMTree"}: {t}s')


if __name__ == '__main__':
    main()

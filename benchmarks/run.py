import sys
from random import Random
import shutil
sys.path.append('.')  # make it runnable from the top level

from src.lsmtree import LSMTree
from src.hybridlog import HybridLog
from src.appendlog import AppendLog

from benchmarks.distributions import Uniform, Zipfian, HotSet
from benchmarks.timer import Timer


def generate_pairs(rng, key_len, val_len, n_items):
    return (
        [rng.randbytes(rng.randint(1, key_len)) for _ in range(n_items)],
        [rng.randbytes(rng.randint(0, val_len)) for _ in range(n_items)]
    )


def main():
    base_dirname = './benchmark_data_'
    n_items = [100, 10_000, 1_000_000]
    n_iter = 1_000_000
    seed = 1

    rng = Random(seed)

    for n_i in n_items:
        print(f'number of distinct choices for keys and values: {n_i}')
        distributions = [Zipfian(items=n_i, seed=1), HotSet(items=n_i, seed=1), Uniform(items=n_i, seed=1)]

        engines = [
            LSMTree(base_dirname + 'lsm'),
            HybridLog(base_dirname + 'hlog'),
            AppendLog(base_dirname + 'alog')
        ]

        keys, vals = generate_pairs(rng, 4, 4, n_i)

        for distro in distributions:
            print(f'distribution:\t{distro.name}')
            all_keys = [keys[next(distro)] for _ in range(n_iter)]
            all_vals = [vals[next(distro)] for _ in range(n_iter)]

            for engine in engines:
                print(f'{n_iter:.1e} writes', end='\t')
                with Timer(name=engine.name, print=True, truncate=True) as t:
                    for k, v in zip(all_keys, all_vals):
                        engine[k] = v
                print(f'{n_iter:.1e} reads', end='\t')
                with Timer(name=engine.name, print=True, truncate=True) as t:
                    for k, v in zip(all_keys, all_vals):
                        engine[k]

        for engine in engines:
            engine.close()
            shutil.rmtree(engine.data_dir)
        print()


if __name__ == '__main__':
    main()

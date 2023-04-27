import shutil
from random import Random
from typing import Type, Any, Optional

from kevo import LSMTree, HybridLog, AppendLog, MemOnly


class FuzzyTester:
    def __init__(self):
        pass

    def fuzzy_test(self,
                   engine_cls: Type[AppendLog] | Type[HybridLog] | Type[LSMTree] | Type[MemOnly],
                   args: dict[str, Any],
                   key_len_range: tuple[int, int] = (1, 10),
                   val_len_range: tuple[int, int] = (0, 10),
                   n_items: int = 100,
                   n_iter: int = 1_000_000,
                   seeds: Optional[list[int]] = None,
                   test_recovery: bool = True,
                   test_replica: bool = True):
        '''
        Choose `n_items` keys with lengths in the range `key_range` and `n_items` values with lengths in the range `val_range`
        Then, perform `n_iter` writes to the engine and a python dict.
        Finally, do `n_iter` reads and check that all reads are consistent with the python dict (which we assume to be correct).
        If `seeds` is given as an iterable, repeat this test for each seed.
        If `test_rebuild`, close the engine and reopen it before the reads to test recovery.
        If `test_replica`, completely remove the data dir before reads and rely on the given replica (as arg) to recover.
        '''
        if not seeds:
            seeds = [1]
        # remove duplicate seeds
        seeds = list(set(seeds))
        for seed in seeds:
            rng = Random(seed)

            engine = engine_cls(**args)

            dict = {}
            keys = [rng.randbytes(rng.randint(*key_len_range)) for _ in range(n_items)]
            values = [rng.randbytes(rng.randint(*val_len_range)) for _ in range(n_items)]

            for _ in range(n_iter):
                rand_key = rng.choice(keys)
                rand_value = rng.choice(values)

                # emulating the kvstore's behaviour ("value 0" == delete) 
                if not rand_value:
                    if rand_key in dict:
                        del dict[rand_key]
                else:
                    dict[rand_key] = rand_value

                engine.set(rand_key, rand_value)

            if test_recovery:
                engine.close()
                if test_replica:
                    shutil.rmtree(engine.data_dir.name)
                engine = engine_cls(**args)

            for k, v in dict.items():
                self.assertEqual(v, engine.get(k))

            engine.close()
            # for all iterations (for every seed), clear the data dir, except for the last one, which will be cleared
            # by the test teardown method
            if seed != seeds[-1]:
                shutil.rmtree(engine.data_dir.name)
                # NOTE i should also destroy the replica here if any

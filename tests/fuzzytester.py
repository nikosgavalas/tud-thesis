import json
import tempfile
import shutil
import os
import base64
from random import Random
from typing import Type, Any, Optional

from kevo import LSMTree, HybridLog, AppendLog, MemOnly


def snapshot_dict(dict, tmpdir, version):
    dict_ser = {base64.b64encode(k).decode(): base64.b64encode(v).decode() for k, v in dict.items()}
    with open(os.path.join(tmpdir, str(version)), 'w') as f:
        f.write(json.dumps(dict_ser))


def load_dict_snapshot(tmpdir, version):
    with open(os.path.join(tmpdir, str(version)), 'r') as f:
        j = json.loads(f.read())
    return {base64.b64decode(k): base64.b64decode(v) for k, v in j.items()}


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
                   test_remote: bool = True):
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
                if test_remote:
                    engine.snapshot(0)
                engine.close()
                if test_remote:
                    shutil.rmtree(engine.data_dir.name)
                engine = engine_cls(**args)

            for k, v in dict.items():
                self.assertEqual(v, engine.get(k))

            engine.close()
            # for all iterations (for every seed), clear the data dir, except for the last one, which will be cleared
            # by the test teardown method
            if seed != seeds[-1]:
                shutil.rmtree(engine.data_dir.name)
                engine.remote.destroy()

    def fuzzy_test_snapshot(self,
                            engine_cls: Type[AppendLog] | Type[HybridLog] | Type[LSMTree] | Type[MemOnly],
                            args: dict[str, Any],
                            key_len_range: tuple[int, int] = (1, 10),
                            val_len_range: tuple[int, int] = (0, 10),
                            n_items: int = 100,
                            n_iter: int = 1_000_000,
                            seed: Optional[int] = None,
                            versions: Optional[int] = 3):
        if not seed:
            seed = 1
        rng = Random(seed)

        engine = engine_cls(**args)

        dict = {}
        keys = [rng.randbytes(rng.randint(*key_len_range)) for _ in range(n_items)]
        values = [rng.randbytes(rng.randint(*val_len_range)) for _ in range(n_items)]

        with tempfile.TemporaryDirectory() as tmpdir:
            for v in range(versions):
                for _ in range(n_iter):
                    rand_key = rng.choice(keys)
                    rand_value = rng.choice(values)

                    if not rand_value:
                        if rand_key in dict:
                            del dict[rand_key]
                    else:
                        dict[rand_key] = rand_value

                    engine.set(rand_key, rand_value)
                # snapshot the engine
                engine.snapshot(v)
                # snapshot the dict
                snapshot_dict(dict, tmpdir, v)

            for v in range(versions):
                engine.restore(v)
                dict = load_dict_snapshot(tmpdir, v)

                for k, v in dict.items():
                    self.assertEqual(v, engine.get(k))

        engine.close()

    def fuzzy_test_snapshot_continuous(self,
                                       engine_cls: Type[AppendLog] | Type[HybridLog] | Type[LSMTree] | Type[MemOnly],
                                       args: dict[str, Any],
                                       key_len_range: tuple[int, int] = (1, 10),
                                       val_len_range: tuple[int, int] = (0, 10),
                                       n_items: int = 100,
                                       n_iter: int = 1_000_000,
                                       seed: Optional[int] = None):
        # write some values, take snapshot v0, write some more, take another snapshot v1, write some more, take
        # snapshot v2
        # then go back to snapshot v0, continue writing from there, take another snapshot that overrides the v1
        # then load all 3 snapshots and check that they are consistent with the dict
        if not seed:
            seed = 1
        rng = Random(seed)

        engine = engine_cls(**args)

        dict = {}
        keys = [rng.randbytes(rng.randint(*key_len_range)) for _ in range(n_items)]
        values = [rng.randbytes(rng.randint(*val_len_range)) for _ in range(n_items)]

        with tempfile.TemporaryDirectory() as tmpdir:
            # write and snapshot 3 times
            for v in range(3):
                for _ in range(n_iter):
                    rand_key, rand_value = rng.choice(keys), rng.choice(values)
                    if not rand_value:
                        if rand_key in dict:
                            del dict[rand_key]
                    else:
                        dict[rand_key] = rand_value
                    engine.set(rand_key, rand_value)

                engine.snapshot(v)
                snapshot_dict(dict, tmpdir, v)

            # simulate a shutdown in between
            engine.close()
            engine = engine_cls(**args)

            # load version 0
            engine.restore(0)
            dict = load_dict_snapshot(tmpdir, 0)
            for k, v in dict.items():
                self.assertEqual(v, engine.get(k))

            # write some more
            for _ in range(n_iter):
                rand_key, rand_value = rng.choice(keys), rng.choice(values)
                if not rand_value:
                    if rand_key in dict:
                        del dict[rand_key]
                else:
                    dict[rand_key] = rand_value
                engine.set(rand_key, rand_value)

            # overwrite snapshot 1
            engine.snapshot(1)
            snapshot_dict(dict, tmpdir, 1)

            # now load all 3 and check that they are ok
            for v in range(3):
                # load version 0
                engine.restore(v)
                dict = load_dict_snapshot(tmpdir, v)
                # check that it is ok
                for k, v in dict.items():
                    self.assertEqual(v, engine.get(k))

        engine.close()

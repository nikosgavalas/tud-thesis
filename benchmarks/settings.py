from kevo import LSMTree, HybridLog, AppendLog

OVERWRITTEN = 'overwritten'


def make_experiments_args(experiment_id):
    experiments = {
        0: [  # basic toy experiment
          [4],    # klens
          [4],    # vlens
          [100],  # n_items_list
          [100]   # n_ops_list
        ],
        1: [  # experiment 1, compare all engines altogether
            [10, 50, 100],
            [100, 500, 1000],
            [100, 10_000, 100_000],
            [1_000, 10_000, 100_000, 1_000_000],
        ]
    }
    return experiments[experiment_id]


def make_distros_args(experiment_id, seed):
    experiments = {
        0: [
            # 'items' overwritten later because they need to match the n_items.
            {'items': OVERWRITTEN, 'seed': [seed]},
            {'items': OVERWRITTEN, 'seed': [seed]},
            {'items': OVERWRITTEN, 'n_sets': [5], 'rotation_interval': [100], 'seed': [seed]}
        ],
        1: [
            {'items': OVERWRITTEN, 'seed': [seed]},
            {'items': OVERWRITTEN, 'seed': [seed]},
            {'items': OVERWRITTEN, 'n_sets': [5], 'rotation_interval': [1_000], 'seed': [seed]}
        ]
    }
    return experiments[experiment_id]


def make_engines_args(experiment_id, base_dir):
    experiments = {
        0: [
            # 'max_key_len' and 'max_value_len' overwritten later because they need to match kvlen
            {
                'data_dir': [base_dir + LSMTree.name],
                'max_key_len': OVERWRITTEN,
                'max_value_len': OVERWRITTEN,
                'max_runs_per_level': [1],
                'density_factor': [50],
                'memtable_bytes_limit': [10_000],
                'replica': [None]
            },
            {
                'data_dir': [base_dir + HybridLog.name],
                'max_key_len': OVERWRITTEN,
                'max_value_len': OVERWRITTEN,
                'max_runs_per_level': [1],
                'mem_segment_len': [2 ** 20],
                'ro_lag_interval': [2 ** 10],
                'flush_interval': [4 * 2 ** 10],
                'hash_index': ['dict'],
                'compaction_enabled': [False],
                'replica': [None]
            },
            {
                'data_dir': [base_dir + AppendLog.name],
                'max_key_len': OVERWRITTEN,
                'max_value_len': OVERWRITTEN,
                'max_runs_per_level': [1],
                'threshold': [4_000_000],
                'replica': [None]
            }
        ],
        1: [
            # 'max_key_len' and 'max_value_len' overwritten later because they need to match kvlen
            {
                'data_dir': [base_dir + LSMTree.name],
                'max_key_len': OVERWRITTEN,
                'max_value_len': OVERWRITTEN,
                'max_runs_per_level': [3, 6, 9],
                'density_factor': [1, 10, 100],
                'memtable_bytes_limit': [100_000, 1_000_000, 10_000_000],
                'replica': [None]
            },
            {
                'data_dir': [base_dir + HybridLog.name],
                'max_key_len': OVERWRITTEN,
                'max_value_len': OVERWRITTEN,
                'max_runs_per_level': [3, 6, 9],
                'mem_segment_len': [100_000, 1_000_000, 10_000_000],
                'ro_lag_interval': [10_000, 100_000, 1_000_000],
                'flush_interval': [10_000, 100_000, 1_000_000],
                'hash_index': ['dict'],
                'compaction_enabled': [False, True],
                'replica': [None]
            },
            {
                'data_dir': [base_dir + AppendLog.name],
                'max_key_len': OVERWRITTEN,
                'max_value_len': OVERWRITTEN,
                'max_runs_per_level': [3, 6, 9],
                'threshold': [100_000, 1_000_000, 10_000_000],
                'replica': [None]
            }
        ]
    }
    return experiments[experiment_id]

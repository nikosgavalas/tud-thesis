import random


class Zipfian:
    """
    Adapted from https://github.com/kPsarakis/sfaas-dataflow/blob/main/demo-ycsb/zipfian_generator.py
    """

    def __init__(self, items: int = None, mn: int = None, mx: int = None, zipf_const: float = 0.99, seed: int = None):
        if seed is not None:
            self.__rng = random.Random(seed)
        else:
            self.__rng = random.Random()

        if items is not None:
            self.__max = items - 1
            self.__min = 0
            self.__items = items
        else:
            self.__max = mx
            self.__min = mn
            self.__items = self.__max - self.__min + 1

        self.__zipf_constant: float = zipf_const

        self.__zeta = self.zeta_static(self.__max - self.__min + 1, self.__zipf_constant)
        self.__base = self.__min
        self.__theta: float = self.__zipf_constant
        zeta2theta = self.zeta(2, self.__theta)
        self.__alpha: float = 1.0 / (1.0 - self.__theta)
        self.__count_for_zeta: int = items
        self.__eta: float = (1 - pow(2.0 / items, 1 - self.__theta)) / (1 - zeta2theta / self.__zeta)
        self.__allow_item_count_decrease: bool = False

    def __next__(self):
        u: float = self.__rng.random()
        uz: float = u * self.__zeta
        if uz < 1.0:
            return self.__base
        if uz < 1.0 + pow(0.5, self.__theta):
            return self.__base + 1
        return self.__base + int(self.__items * pow(self.__eta * u - self.__eta + 1, self.__alpha))

    def __iter__(self):
        return self

    def zeta(self, *params):
        if len(params) == 2:
            n, theta_val = params
            self.__count_for_zeta = n
            return self.zeta_static(n, theta_val)
        elif len(params) == 4:
            st, n, theta_val, initial_sum = params
            self.__count_for_zeta = n
            return self.zeta_static(n, theta_val, theta_val, initial_sum)

    def zeta_static(self, *params):
        if len(params) == 2:
            n, theta = params
            st = 0
            initial_sum = 0
            return self.zeta_sum(st, n, theta, initial_sum)
        elif len(params) == 4:
            st, n, theta, initial_sum = params
            return self.zeta_sum(st, n, theta, initial_sum)

    @staticmethod
    def zeta_sum(st, n, theta, initial_sum):
        s = initial_sum
        for i in range(st, n):
            s += 1 / (pow(i + 1, theta))
        return s


class Uniform:
    def __init__(self, items: int, seed: int = None):
        if seed is not None:
            self.rng = random.Random(seed)
        else:
            self.rng = random.Random()
        assert items > 0
        self.items = items

    def __next__(self):
        return self.rng.randint(0, self.items - 1)    


class HotSet(Uniform):
    def __init__(self, items: int, n_sets: int = 5, rotation_interval: int = 100, seed: int = None):
        # divides the items into n ranges
        # at every interval, a range is selected (round-robin). Items from this range are (uniformly withing the range)
        # selected with 90% probability, and from the other ranges with 10% probability.
        super().__init__(items, seed)
        assert items >= n_sets
        self.n_sets = n_sets
        self.rotation_interval = rotation_interval

        self.sets = self._split(list(range(items)), self.n_sets)
        self.hot_set_idx = 0
        self.rot_counter = 0

    def _split(self, iterable, n_slices):
        k, m = divmod(len(iterable), n_slices)
        return [iterable[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n_slices)]

    def __next__(self):
        if self.rot_counter == self.rotation_interval:  # switch hot set
            self.hot_set_idx = (self.hot_set_idx + 1) % self.n_sets
            self.rot_counter = 0

        if self.rng.randint(0, 9) == 0: # cold set - 10% prob
            set_idx = self.rng.randint(0, self.n_sets - 2)
            set_idx = set_idx if set_idx < self.hot_set_idx else set_idx + 1  # correct the idx
        else: # hot set - 90% prob
            set_idx = self.hot_set_idx

        set = self.sets[set_idx]
        idx = self.rng.randint(0, len(set) - 1)
        item = set[idx]

        self.rot_counter += 1

        return item


if __name__ == "__main__":
    import matplotlib.pyplot as plt
    import numpy as np

    z = Zipfian(items=10, seed=1)
    u = Uniform(items=10, seed=1)
    h = HotSet(items=10, seed=1)

    # counts = np.array([next(z) for _ in range(2000)])
    # counts = np.array([next(u) for _ in range(2000)])
    counts = np.array([next(h) for _ in range(600)])
    plt.hist(counts, density=False, bins=50)  # density=False would make counts (unnormalized)
    plt.ylabel('Counts')
    plt.xlabel('Data')
    plt.show()

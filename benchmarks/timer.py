import time


class Timer:
    def __init__(self, name='', print=False, truncate=True):
        self.name = name
        self.print = print
        self.truncate = truncate

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval  = self.end - self.start
        if self.print:
            print(self.__str__())

    def __str__(self):
        interval = f'{self.interval:.3f}' if self.truncate else f'{self.interval}'
        return f'{self.name}:\t{interval}s' if self.name else interval

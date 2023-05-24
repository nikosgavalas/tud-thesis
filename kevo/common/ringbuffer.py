from sys import getsizeof


class RingBuffer:
    '''
    __getitem__(), __setitem__() here are specific to my use case (to this
    codebase) and would not be present in a generic ring buffer implementation
    '''
    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError('capacity has to be > 0')
        self.capacity = capacity
        self.buffer = [None] * capacity
        self.write_idx = 1
        self.read_idx = 0

    def __len__(self):
        return self.write_idx - self.read_idx - 1

    def __getitem__(self, key):
        if key >= self.write_idx or key <= self.read_idx:
            raise IndexError('index out of bounds')
        return self.buffer[key % self.capacity]

    def __setitem__(self, key, value):
        if key >= self.write_idx or key <= self.read_idx:
            raise IndexError('index out of bounds')
        self.buffer[key % self.capacity] = value

    def is_empty(self):
        return self.__len__() == 0

    def is_full(self):
        return self.__len__() == self.capacity

    def add(self, element):
        if self.is_full():
            raise BufferError('buffer full')
        self.buffer[self.write_idx % self.capacity] = element
        ret = self.write_idx
        self.write_idx += 1
        return ret

    def pop(self):
        if self.is_empty():
            raise BufferError('buffer empty')
        self.read_idx += 1
        return self.buffer[self.read_idx % self.capacity]

    def set_tail_offset(self, offset: int):
        self.write_idx = offset + 1
        self.read_idx = offset

    def __sizeof__(self):
        return getsizeof(self.buffer)

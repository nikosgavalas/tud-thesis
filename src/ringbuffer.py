
class RingBuffer:
    def __init__(self, capacity: int):
        assert capacity > 0
        self.capacity = capacity
        self.buffer = [None] * capacity
        self.write_idx = 1
        self.read_idx = 0

    def __len__(self):
        return self.write_idx - self.read_idx - 1

    def is_empty(self):
        return self.__len__() == 0

    def is_full(self):
        return self.__len__() == self.capacity

    def add(self, element):
        if self.is_full():
            raise BufferError('buffer full')
        self.buffer[self.write_idx % self.capacity] = element
        self.write_idx += 1

    def pop(self):
        if self.is_empty():
            raise BufferError('buffer empty')
        self.read_idx += 1
        return self.buffer[self.read_idx % self.capacity]

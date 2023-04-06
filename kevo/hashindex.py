'''
Instead of using 15-bit tags and records allocated by a memory allocator (like jmalloc), to which we would point directly
using 48-bit addresses, I simplify the implementation a bit, using 32-bit keys, 32-bit values and no mem allocator,
as I am storing the keys and the values directly to the bucket entries - no need for pointing the entries to records
and then storing the values to these records.
each bucket entry has the key written in the lower bits so that i can do the lookup by key faster (no need to shift): | value | key |
'''

from mmh3 import hash  # 32-bit signed int


class _HashIndex:
    def __init__(self, n_buckets_power=4, key_len_bits=32, value_len_bits=32):
        assert n_buckets_power >= 4
        assert key_len_bits > 0
        assert value_len_bits > 0

        self.n_buckets_power = n_buckets_power
        self.n_buckets = 2**n_buckets_power
        self.key_len_bits = key_len_bits
        self.value_len_bits = value_len_bits

        self.value_mask_lower = (1 << self.value_len_bits) - 1
        self.value_mask_upper = self.value_mask_lower << self.key_len_bits
        self.key_mask_lower = (1 << self.key_len_bits) - 1
        self.bucket_mask = (1 << self.n_buckets_power) - 1

        self.bucket_len = 8
        self.buckets = [[0] * self.bucket_len for _ in range(self.n_buckets)]
        self.last_bucket = self.n_buckets - 1

        self.entries_cnt = 0
        self.capacity = 2**self.n_buckets_power * self.bucket_len
        self.resizing_threshold = int(0.75 * self.capacity)

    def _get_bucket_index(self, key):
        h = hash(key, signed=False)
        bucket_idx = h & self.bucket_mask
        assert bucket_idx < self.n_buckets
        return bucket_idx

    def _bytes_to_int(self, bytes):
        return int.from_bytes(bytes)

    def _lookup_bucket(self, bucket_idx, by_key=None, by_value=None, by_empty=False):
        for entry_idx in range(self.bucket_len - 1):
            entry = self.buckets[bucket_idx][entry_idx]
            if by_key is not None and entry & self.key_mask_lower == by_key:
                return bucket_idx, entry_idx
            if by_empty and entry == 0:
                return bucket_idx, entry_idx
            if by_value is not None and (entry >> self.key_len_bits) & self.value_mask_lower == by_value:
                return bucket_idx, entry_idx
        next_bucket = self.buckets[bucket_idx][self.bucket_len - 1]
        if next_bucket != 0:
            return self._lookup_bucket(next_bucket, by_key=by_key, by_value=by_value)
        return bucket_idx, None  # last bucket reached and no entry

    def __getitem__(self, key):
        # print(key)
        assert type(key) is bytes and len(key) <= self.key_len_bits // 8
        bucket_idx = self._get_bucket_index(key)
        key = self._bytes_to_int(key)
        bucket_idx, entry_idx = self._lookup_bucket(bucket_idx, by_key=key)
        if entry_idx is not None:
            return (self.buckets[bucket_idx][entry_idx] & self.value_mask_upper) >> self.key_len_bits
        return None

    def __setitem__(self, key, value):
        # print(key, value)
        assert type(key) is bytes and len(key) <= self.key_len_bits // 8 and type(value) is int
        bucket_idx = self._get_bucket_index(key)
        key = self._bytes_to_int(key)
        bucket_idx, entry_idx = self._lookup_bucket(bucket_idx, by_key=key)
        if entry_idx is not None:
            # if item in the buckets, update in place
            if value == 0:
                self.buckets[bucket_idx][entry_idx] = 0
                self.entries_cnt -= 1
            else:
                self.buckets[bucket_idx][entry_idx] = (value << self.key_len_bits) | key
            return
        if value == 0:  # do not insert empty values (equiv to delete)
            return
        # if not, find an empty slot and write it
        bucket_idx, entry_idx = self._lookup_bucket(bucket_idx, by_empty=True)
        if entry_idx is not None:
            self.buckets[bucket_idx][entry_idx] = (value << self.key_len_bits) | key
            self.entries_cnt += 1
            return
        # if not empty slot, allocate a new bucket and write it there
        self.buckets.append([0] * self.bucket_len)
        self.last_bucket += 1
        # update the last bucket to point to the newly added bucket
        self.buckets[bucket_idx][self.bucket_len - 1] = self.last_bucket
        # actually insert the new value
        self.buckets[self.last_bucket][0] = (value << self.key_len_bits) | key
        # increment the entries counter
        self.entries_cnt += 1
        return

    def __delitem__(self, key):
        # set value to zero
        self.__setitem__(key, 0)

    def __contains__(self, key):
        return self.__getitem__(key) is not None


class HashIndex:
    def __init__(self, n_buckets_power=4, key_len_bits=32, value_len_bits=32):
        self._hash_index = _HashIndex(n_buckets_power=n_buckets_power, key_len_bits=key_len_bits, value_len_bits=value_len_bits)

        self.n_buckets_power = n_buckets_power
        self.key_len_bits = key_len_bits
        self.value_len_bits = value_len_bits

    def __getitem__(self, key):
        return self._hash_index.__getitem__(key)

    def __setitem__(self, key, value):
        self._hash_index.__setitem__(key, value)
        # check if needs resizing
        if self._hash_index.entries_cnt > self._hash_index.resizing_threshold:
            self.resize()

    def __delitem__(self, key):
        return self._hash_index.__delitem__(key)

    def __contains__(self, key):
        return self._hash_index.__contains__(key)

    def resize(self):
        new_hash_index = _HashIndex(n_buckets_power=self.n_buckets_power + 1,
            key_len_bits=self.key_len_bits,
            value_len_bits=self.value_len_bits)

        for bucket in self._hash_index.buckets:
            for entry in bucket:
                key = entry & self._hash_index.key_mask_lower
                value = entry >> self._hash_index.key_len_bits
                if value == 0:
                    continue
                new_hash_index[key.to_bytes(self._hash_index.key_len_bits // 8)] = value

        self._hash_index = new_hash_index

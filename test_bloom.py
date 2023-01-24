'''
Crappy test to quickly make sure that I didn't screw up the implementation of the BloomFilter
'''

from bloom import BloomFilter
skip = 50
n = 1000000
b = BloomFilter(int(n / skip), 0.001)
for i in range(n):
    s = (str(i)).encode()
    if i % skip == 0:
        b.add(s)
fp_count = 0
for i in range(n):
    s = (str(i)).encode()
    if i % skip == 0 and s not in b:
        print(f'{i} in set but BloomFilter says it is not (very bad)')
    elif i % skip != 0 and s in b:
        fp_count += 1
        # print(f'{i} not in set but BloomFilter says it is (false positive)')
print(fp_count / n)  # should be close to the false pos rate set above

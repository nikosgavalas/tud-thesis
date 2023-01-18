from lsmtree import LSMTree

l = LSMTree()
l.set(b'b', b'2')
l.set(b'asdf', b'12345')
l.set(b'cc', b'cici345')
l.flush_memtable()


# def merge():
#     with open('data/L0', 'r') as L0, open('data/L1', 'r') as L1, open('data/L2', 'w') as L2:
#         while True:
#             l0, l1 = L0.readline(), L1.readline()
#             if l0 == '':
#                 L2.write(l1)
#                 L2.write(L1.read())
#                 break
#             if l1 == '':
#                 L2.write(l0)
#                 L2.write(L0.read())
#                 break
#             if l0 < l1:
#                 L2.write(l0)
#                 L2.write(l1)
#             else:
#                 L2.write(l1)
#                 L2.write(l0)

# merge()

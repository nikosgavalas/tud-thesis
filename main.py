import sys
import csv
import signal
from argparse import ArgumentParser

from lsmtree import LSMTree


# l = LSMTree()
# l.set(b'b', b'2')
# l.set(b'asdf', b'12345')
# l.set(b'cc', b'cici345')
# l.set(b'b', b'3')
# # l.flush_memtable()

# print(l.get(b'b'))
# print(l.get(b'asdf'))
# print(l.get(b'cc'))

# l.merge()


# test 2
# l.set(b'a1', b'a1')
# l.set(b'a1', b'a11')
# l.set(b'a2', b'a2')

# l.set(b'a2', b'a22')
# l.set(b'a3', b'a3')
# l.set(b'a4', b'a4')

# l.set(b'a3', b'a31')
# l.set(b'a5', b'a5')
# l.set(b'a6', b'a6')

# l.merge()


def write_exit_msg():
    if sys.stdout.isatty():
        sys.stdout.write('Use q or Ctrl-D to exit.\n')
        sys.stdout.flush()


def signal_handler(sig, frame):
    write_exit_msg()


def parse(fd):
    db = LSMTree()
    csv_reader = csv.reader(fd, delimiter=' ', quotechar='"')
    for row in csv_reader:
        if not row:
            continue
        op = row[0]
        if op == 'w':
            key = row[1]
            value = row[2]
            db.set(key.encode(), value.encode())
        elif op == 'r':
            key = row[1]
            sys.stdout.write(db.get(key.encode()).decode())
            sys.stdout.write('\n')
            sys.stdout.flush()
        elif op == 'q':
            return


def main():
    signal.signal(signal.SIGINT, signal_handler)

    parser = ArgumentParser()
    parser.add_argument('-f', type=str, help='path of the input file')
    args = parser.parse_args()

    write_exit_msg()

    if args.f:
        with open(args.f, 'r') as fd:
            parse(fd)
    else:
        parse(sys.stdin)


if __name__ == '__main__':
    main()

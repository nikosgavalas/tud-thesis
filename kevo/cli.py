import sys
import csv
import signal
from argparse import ArgumentParser

from kevo.lsmtree import LSMTree
from kevo.hybridlog import HybridLog
from kevo.appendlog import AppendLog


def write_exit_msg():
    if sys.stdout.isatty():
        sys.stdout.write('Use q or Ctrl-D to exit.\n')
        sys.stdout.flush()


def signal_handler(sig, frame):
    write_exit_msg()


def parse(fd, args):
    if args.engine == 'lsmtree':
        db = LSMTree(args.d, args.runs_per_level, args.density_factor, args.memory_limit)
    elif args.engine == 'hybridlog':
        db = HybridLog(args.d, args.max_key_len, args.max_val_len, args.memory, args.ro_lag, args.flush_interval)
    elif args.engine == 'appendlog':
        db = AppendLog(args.d, args.runs_per_level, args.threshold)
 
    csv_reader = csv.reader(fd, delimiter=' ', quotechar='"')
    for row in csv_reader:
        if not row:
            continue
        try:
            op = row[0]
            if op == 's' or op == 'w':
                key = row[1]
                value = row[2]
                db.set(key.encode(), value.encode())
            elif op == 'd':
                key = row[1]
                db.set(key.encode(), b'')
            elif op == 'g' or op == 'r':
                key = row[1]
                sys.stdout.write(db.get(key.encode()).decode())
                sys.stdout.write('\n')
                if sys.stdout.isatty():
                    sys.stdout.flush()
            elif op == 'q':
                db.close()
                return
        except IndexError:
            sys.stderr.write('malformed command.\n')


def main():
    signal.signal(signal.SIGINT, signal_handler)

    parser = ArgumentParser()
    subparsers = parser.add_subparsers(title='engine', dest='engine', help='engine selection')
    parser.add_argument('-f', type=str, help='path to input file')
    parser.add_argument('-d', type=str, help='path to data directory', default='./data')

    lsm_parser = subparsers.add_parser('lsmtree')
    lsm_parser.add_argument('--runs-per-level', type=int, help='max runs per level', default=3)
    lsm_parser.add_argument('--density-factor', type=int, help='density factor', default=20)
    lsm_parser.add_argument('--memory-limit', type=int, help='memtable bytes limit', default=10**6)

    hybridlog_parser = subparsers.add_parser('hybridlog')
    hybridlog_parser.add_argument('--max-key-len', type=int, help='max key length', default=4)
    hybridlog_parser.add_argument('--max-val-len', type=int, help='max value length', default=4)
    hybridlog_parser.add_argument('--memory', type=int, help='memory segment length in bytes', default=2**20)
    hybridlog_parser.add_argument('--ro-lag', type=int, help='read-only lag interval in num. of records', default=2**10)
    hybridlog_parser.add_argument('--flush-interval', type=int, help='flush interval in num. of records', default=(4 * 2**10))
    hybridlog_parser.add_argument('--hash-index', type=str, help='type of the hash index, native or dict', default='dict')
    hybridlog_parser.add_argument('--compaction-interval', type=int, help='compaction interval in num of flushes. If zero, compaction is disabled (default)', default=0)

    appendlog_parser = subparsers.add_parser('appendlog')
    appendlog_parser.add_argument('--runs-per-level', type=int, help='max runs per level', default=3)
    appendlog_parser.add_argument('--threshold', type=int, help='threshold that triggers merging, in bytes', default=(4 * 10**6))

    args = parser.parse_args()
    if not args.engine:
        parser.print_help()
        sys.exit(0)

    write_exit_msg()

    if args.f:
        with open(args.f, 'r') as fd:
            parse(fd, args)
    else:
        parse(sys.stdin, args)


if __name__ == '__main__':
    main()

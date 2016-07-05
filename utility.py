import sys


def flush_print(line):
    """
    Print with unbuffered output
    """
    print(line)
    sys.stdout.flush()


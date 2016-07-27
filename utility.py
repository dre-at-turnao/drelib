import socket
import sys


def flush_print(line):
    """
    Print with unbuffered output
    """
    print(line)
    sys.stdout.flush()

def get_ip(hostname):
    """
    Return the ip for the given hostname
    """
    return socket.gethostbyname(hostname)


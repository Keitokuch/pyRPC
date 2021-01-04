import time
from typing import Tuple

from .exceptions import *


__all__ = ['log', 'load_addr']

log_init = False

def timestamp():
    return time.time_ns()
    #  from datetime import datetime
    #  return datetime.now().isoformat(timespec='milliseconds')


def log(event, msg):
    global log_init
    if not log_init:
        print("==================== Begin Log =====================", flush=True)
        log_init = True
    print("{: <10} {} {}".format(f"[{event}]", timestamp(), msg), flush=True)


def dump_address(addr):
    return addr[0] + ":" + addr[1]


def load_addr(hostport: str) -> Tuple[str, str]:
    try:
        host, port = hostport.split(':')
        return host, port
    except ValueError:
        raise RPCError("port number not found in hostport {}".format(hostport))

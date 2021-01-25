import asyncio
import time
import threading
from typing import Tuple

from .exceptions import *


__all__ = ['log', 'load_addr', 'start_loop_in_thread']

log_init = False

def timestamp():
    return time.time_ns()
#  from datetime import datetime
#  return datetime.now().isoformat(timespec='milliseconds')

def async_worker(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def start_loop_in_thread(loop=None, daemon: bool=True, debug: bool=None):
    loop = loop or asyncio.new_event_loop()
    if debug is not None:
        loop.set_debug(debug)
    loop_thread = threading.Thread(target=async_worker, args=(loop,))
    loop_thread.daemon = daemon
    loop_thread.start()
    return loop


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

import asyncio
from functools import wraps

from .utils import start_loop_in_thread

REMOTE = "remote"
LOCAL = "local"
ONLINE = "online"
REMOTE_NODE = "remote_node"

SERVICE_RPC = "_service_rpc"

RPC_FLAG = "_rpc"
SYNC_FLAG = "_sync"
TASK_FLAG = "_task"


def rpc__(f):
    setattr(f, RPC_FLAG, True)
    return f


def is_rpc(f):
    return hasattr(f, RPC_FLAG)


def is_service_rpc(f):
    return hasattr(f, SERVICE_RPC)


def is_sync(f):
    return hasattr(f, SYNC_FLAG)


def is_task(f):
    return hasattr(f, TASK_FLAG)


rpc_loop = None
def get_event_loop():
    global rpc_loop
    if rpc_loop is None:
        rpc_loop = asyncio.new_event_loop()
    return rpc_loop


def sync_await(coro, loop):
    if not loop.is_running():
        start_loop_in_thread(loop=loop, daemon=True)
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result()


#  Adapt an async function for sync call
def make_sync(async_func, loop):
    if asyncio.iscoroutinefunction(async_func):
        @wraps(async_func)
        def sync_func(*args, **kwargs):
            return sync_await(async_func(*args, **kwargs), loop)
        return sync_func
    return async_func

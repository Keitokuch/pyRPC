import asyncio
import queue
import threading
from functools import wraps

from .exceptions import *

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


class Request:

    def __init__(self, method, client, server, id=None, args=(), kwargs={}):
        self.rpc_name = method
        self.client = client
        self.server = server
        self.id = id
        self.args = args
        self.kwargs = kwargs


class Response():

    def __init__(self, rpc_name, client, server, id, status="error", result=None, exception=None):
        self.rpc_name = rpc_name
        self.client = client
        self.server = server
        self.id = id
        self.status = status
        self.result = result
        self.exception = exception

    @classmethod
    def for_request(cls, request):
        return cls(request.rpc_name, request.client, request.server, request.id)


def get_event_loop():
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


#  Await a coroutine synchronously using another thread
def sync_await(coro):
    async def thread_await(coro, return_Q):
        try:
            ret = await coro
        except Exception as e:
            return_Q.put(e)
            return
        return_Q.put(ret)
        #  asyncio.get_running_loop().stop()
    return_Q = queue.Queue()
    async_call = thread_await(coro, return_Q)
    thread = threading.Thread(target=asyncio.run, args=(async_call, ))
    thread.start()
    thread.join()
    ret = return_Q.get()
    if isinstance(ret, Exception):
        raise ret
    else:
        return ret


#  Adapt an async function for sync call
def make_sync(async_func):
    if asyncio.iscoroutinefunction(async_func):
        @wraps(async_func)
        def sync_func(*args, **kwargs):
            return sync_await(async_func(*args, **kwargs))
        return sync_func
    return async_func

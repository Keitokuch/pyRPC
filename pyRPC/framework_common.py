import asyncio
from functools import wraps
import queue
import threading
from concurrent.futures import ThreadPoolExecutor

from .exceptions import *

REMOTE = "remote"
LOCAL = "local"
ONLINE = "online"
REMOTE_NODE = "remote_node"

SERVICE_RPC = "_service_rpc"

RPC_FLAG = "_rpc"
SYNC_FLAG = "_sync"
TASK_FLAG = "_task"

SENTINEL = b'\x00\x00SENtiNeL\x00\x00ValUe\x00\x00'


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

    def __init__(self, method, id=-1, args=(), kwargs={}, data={}):
        self.method = method
        self.id = id
        self.args = args
        self.kwargs = kwargs
        self.data = data


class Response():

    def __init__(self, method, id, status="success", result=None, error=None, data={}):
        self.method = method
        self.id = id
        self.status = status
        self.result = result
        self.error = error
        self.data = data

    @classmethod
    def for_request(cls, request):
        return cls(request.method, request.id)


def get_event_loop():
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop

#  Await a coroutine synchronously using another thread
def sync_awaitt(coro):
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

pool = ThreadPoolExecutor(max_workers=2)

def async_thread(loop):
    loop.run_forever()

#  loop = asyncio.new_event_loop()
#  thread = threading.Thread(target=async_thread, args=(loop,))
def sync_await(coro):
    def thread_await(coro):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(coro)
    global pool
    future = pool.submit(thread_await, coro)
    return future.result()
    #  with ThreadPoolExecutor() as pool:
    #      res = future.result()
    #      return res

#  Adapt an async function for sync call
def make_sync(async_func):
    if asyncio.iscoroutinefunction(async_func):
        @wraps(async_func)
        def sync_func(*args, **kwargs):
            return sync_await(async_func(*args, **kwargs))
        return sync_func
    return async_func

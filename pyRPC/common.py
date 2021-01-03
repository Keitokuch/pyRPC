import asyncio
from functools import wraps

from .framework_common import *

__all__ = ['rpc', 'task', 'sync_await', 'sync']


def rpc(f):
    setattr(f, SERVICE_RPC, True)
    return rpc__(f)


def task(f):
    @wraps(f)
    async def async_wrapper(self, *args, **kwargs):
        return f(self, *args, **kwargs)

    @wraps(f)
    def sync_wrapper(self, *args, **kwargs):
        return f(self, *args, **kwargs)
    wrapper = sync_wrapper if asyncio.iscoroutinefunction(f) else async_wrapper
    setattr(wrapper, TASK_FLAG, True)
    return wrapper


def sync(f):
    setattr(f, SYNC_FLAG, True)
    return f

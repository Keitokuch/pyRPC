import asyncio
from functools import wraps
from typing import Any, Optional

from .framework_common import *

__all__ = ['rpc', 'task', 'sync', 'Request', 'Response']


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


class Request:

    def __init__(self, method: str, id: Optional[int]=-1, args: tuple=(), kwargs: dict={}, data: dict={}):
        self.method:    str = method
        self.id:        Optional[int] = id
        self.args:      tuple = args
        self.kwargs:    dict = kwargs
        self.data:      dict = data

    def __str__(self):
        return "<{}, {}>".format(self.id, self.method)

    def __getstate__(self):
        state = { 'm': self.method, 'i': self.id }
        if self.args: state['a'] = self.args
        if self.kwargs: state['k'] = self.kwargs
        if self.data: state['d'] = self.data
        return state

    def __setstate__(self, state):
        self.method = state['m']
        self.id = state['i']
        self.args = state.get('a', ())
        self.kwargs = state.get('k', {})
        self.data = state.get('d', {})


class Response():

    def __init__(self, method: str, id: Optional[int], status: str="success", result: Any=None, error: Optional[Exception]=None, data: dict={}):
        self.method:    str = method
        self.id:        Optional[int] = id
        self.status:    str = status
        self.result:    Any = result
        self.error:     Optional[Exception] = error
        self.data:      dict[str, Any] = data

    def __str__(self):
        return "<{}, {}> {}".format(self.id, self.status, self.result)

    @classmethod
    def for_request(cls, request: Request):
        return cls(request.method, request.id)

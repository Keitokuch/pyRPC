import asyncio
from types import MethodType

from . import config

from .utils import *
from .exceptions import *
from .common import *
from .framework_common import *

__all__ = ['RPCClient']


class RPCClient():
    def __init__(self, hostport=None, host=None, port=None, loop=None, blocking=None, rpc_timeout=None, network_timeout=None, protocol=None, **kwargs):
        if hostport:
            host, port = load_addr(hostport)
        self._host = host
        self._port = port
        self._loop = loop
        self._req_num = 0
        self._blocking = blocking if blocking is not None else config.blocking
        self._rpc_timeout = rpc_timeout if rpc_timeout is not None else config.rpc_timeout
        self._network_timeout = network_timeout if network_timeout is not None else config.nw_timeout
        self._conn = None
        self._conn_lock = None
        protocol = protocol or config.protocol
        self.__protocol_factory = protocol['client'] if isinstance(protocol, dict) else protocol
        self.__received = set()
        _ = kwargs

    def connect(self, hostport=None, host=None, port=None):
        if hostport:
            host, port = load_addr(hostport)
        self._host = host or self._host
        self._port = port or self._port
        if not self._port:
            raise RPCError("port number must be provided to connect to server")
        self.close()
        return self

    async def _get_protocol(self):
        if self._conn is None or self._conn[0].is_closing():
            if not self._port:
                raise RPCError("port number must be provided to connect to server")
            try:
                transport, protocol = await asyncio.get_event_loop().create_connection(
                    self.__protocol_factory, host=self._host, port=self._port)
                self._conn = transport, protocol
            except Exception as e:
                raise RPCConnectionError(
                    f"Failed connecting to server ({self._host}, {self._port}).") from e
        return self._conn[1]

    def close(self):
        if self._conn is not None:
            transport, _ = self._conn
            if not transport.is_closing():
                transport.close()
                self._conn = None

    async def async_call(self, method, *args, **kwargs):
        request = Request(method, None, args, kwargs)
        response = await self._remote_call(request)
        if response.error:
            raise response.error
        return response.result

    def call(self, method, *args, **kwargs):
        def blocking_call(self, method, *args, **kwargs):
            request = Request(method, None, args, kwargs)
            response = sync_await(self._remote_call(request), self._loop)
            if response.error:
                raise response.err
            return response.result

        async def async_call(self, method, *args, **kwargs):
            request = Request(method, None, args, kwargs)
            response = await self._remote_call(request)
            if response.error:
                raise response
            return response.error

        if self._blocking:
            self._loop = self._loop or get_event_loop()
            setattr(self, 'call', MethodType(blocking_call, self))
        else:
            setattr(self, 'call', MethodType(async_call, self))
        return self.call(method, *args, **kwargs)

    async def _remote_call(self, request, response_Q=None, exception_Q=None):
        self._conn_lock = self._conn_lock or asyncio.Lock()
        async with self._conn_lock:
            protocol = await self._get_protocol()
        if request.id is None:
            request.id = self._req_num
            self._req_num += 1
        response = Response.for_request(request)

        #  Send rpc request
        try:
            self._on_rpc_call(request)
            response = await asyncio.wait_for(
                protocol.request_one(request), timeout=self._network_timeout)
            self._on_rpc_returned(response)
        except asyncio.TimeoutError:
            err = RPCConnectionTimeoutError(f"Timed out waiting for response")
            if exception_Q:
                await exception_Q.put(err)
            response.error = err
            return response
        if response_Q:
            await response_Q.put(response)
        return response

    def _on_rpc_call(self, request):
        log("request", f"Sent request {request}")

    def _on_rpc_returned(self, response):
        status = response.status if response.id not in self.__received else "discard"
        log(status, f"Received {response}")
        self.__received.add(response.id)

    #  Set timeouts
    def set_rpc_timeout(self, timeout):
        self._rpc_timeout = timeout
        return self

    def set_network_timeout(self, timeout):
        self._network_timeout = timeout
        return self

    def __reduce__(self):
        return (RPCClient, (self._tag,))

    def __str__(self):
        return "RPCClient({}, {})".format(self._host, self._port)

    #  def __getstate__(self):
    #      return {"_tag": self._tag, "_host": self._host, "_port": self._port}

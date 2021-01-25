import asyncio
import pickle
from pyRPC.protocols import RPCClientProtocol

from .server import RPCServer
from .utils import *
from .config import *
from .exceptions import *
from .common import *
from .framework_common import *


RPC_TIMEOUT=3
NW_TIMEOUT=3


class RPCClient():

    def __init__(self, hostport=None, host=None, port=None, loop=None, debug=False, rpc_timeout=RPC_TIMEOUT, network_timeout=NW_TIMEOUT, protocol=None, **kwargs):
        if hostport:
            host, port = load_addr(hostport)
        self._req_num = 0
        self._rpc_timeout = rpc_timeout
        self._network_timeout = network_timeout
        self._host = host
        self._port = port
        self._loop = loop or asyncio.get_event_loop()
        self._debug = debug
        self._conn = None
        self._protocol_factory = protocol or RPCClientProtocol
        self.__received = set()

    def connect(self, hostport=None, host=None, port=None):
        if hostport:
            host, port = load_addr(hostport)
        self._host = host or self._host
        self._port = port or self._port
        if not self._port:
            raise RPCError("port number must be provided to connect to server")
        self.close()
        return self

    #  async def _get_connection(self):
    #      if self._conn is None or self._conn[0].at_eof() or self._conn[1].is_closing():
    #          if not self._port:
    #              raise RPCError("port number must be provided to connect to server")
    #          try:
    #              reader, writer = await asyncio.open_connection(
    #                  host=self._host, port=self._port)
    #          except Exception as e:
    #              raise RPCConnectionError(
    #                  f"Failed connecting to server ({self._host}, {self._port}).") from e
    #          self._conn = reader, writer
    #          #  self._connection = self._protocol()
    #      return self._conn

    async def _get_protocol(self):
        if self._conn is None or self._conn[0].is_closing():
            if not self._port:
                raise RPCError("port number must be provided to connect to server")
            try:
                #  reader, writer = await asyncio.open_connection(
                #      host=self._host, port=self._port)
                transport, protocol = await self._loop.create_connection(
                    self._protocol_factory, host=self._host, port=self._port)
            except Exception as e:
                raise RPCConnectionError(
                    f"Failed connecting to server ({self._host}, {self._port}).") from e
            self._conn = transport, protocol
            #  self._connection = self._protocol()
        return self._conn[1]

    def close(self):
        if self._conn is not None:
            _, writer = self._conn
            if not writer.is_closing():
                writer.close()
                self._conn = None

    async def async_call(self, method, *args, **kwargs):
        request = Request(method, None, args, kwargs)
        response = await self._remote_call(request)
        if response.error:
            raise response.error
        return response.result

    def call(self, method, *args, **kwargs):
        request = Request(method, None, args, kwargs)
        response = sync_await(self._remote_call(request), self._loop)
        if response.error:
            raise response.error
        return response.result

    async def _remote_call(self, request, response_Q=None, exception_Q=None):
        #  reader, writer = await self._get_connection()
        protocol = await self._get_protocol()
        if request.id is None:
            request.id = self._req_num
            self._req_num += 1
        response = Response.for_request(request)

        #  Send rpc request
        self._on_rpc_call(request)
        #  await protocol.write_request(request)
        #  Receive rpc response and close connection
        try:
            #  response = await asyncio.wait_for(
            #      protocol.read_response(), timeout=self._network_timeout)
            response = await asyncio.wait_for(protocol.call_one(request), timeout=self._network_timeout)
        except asyncio.TimeoutError:
            err = RPCConnectionTimeoutError(f"Timed out waiting for response")
            if exception_Q:
                await exception_Q.put(err)
            else:
                raise err
            response.error = err
            return response

        self._on_rpc_returned(response)
        if response_Q:
            await response_Q.put(response)
        return response

    def _on_rpc_call(self, request):
        log("request", f"Sent request {request}")

    def _on_rpc_returned(self, response):
        status = response.status if response.id not in self.__received else "discard"
        #  res_desc = f"<{self._tag}, {response.server._tag}, {response.id}, {response.status}> {response.result}"
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

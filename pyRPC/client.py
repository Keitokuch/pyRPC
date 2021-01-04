import asyncio
import pickle

from .server import RPCServer
from .util import *
from .exceptions import *
from .common import *
from .framework_common import *


RPC_TIMEOUT=3
NW_TIMEOUT=3


class RPCClient():

    def __init__(self, hostport=None, host=None, port=None, rpc_timeout=RPC_TIMEOUT, network_timeout=NW_TIMEOUT, **kwargs):
        if hostport:
            host, port = load_addr(hostport)
        self._req_num = 0
        self._rpc_timeout = rpc_timeout
        self._network_timeout = network_timeout
        self._host = host
        self._port = port
        self._conn = None
        self.__received = set()

    def sync_connect(self, server=None, host=None, port=None):
        sync_await(self.connect(server, host, port))
        return self

    def connect(self, server=None, host=None, port=None):
        if server:
            if isinstance(server, str):
                host, port = load_addr(server)
            elif isinstance(server, RPCServer):
                host = server._host
                port = server._port
        self._host = host or self._host
        self._port = port or self._port
        if not self._port:
            raise RPCError("port number must be provided to connect to server")
        self.close()
        return self

    async def _connect(self):
        if not self._conn or self._conn[1].is_closing():
            if not self._port:
                raise RPCError("port number must be provided to connect to server")
            try:
                reader, writer = await asyncio.open_connection(
                    host=self._host, port=self._port)
            except Exception as e:
                raise RPCConnectionError(
                    f"Failed connecting to server ({self._host}, {self._port}): {e}.") from e
            self._conn = reader, writer
        return self._conn

    def close(self):
        if self._conn is not None:
            _, writer = self._conn
            if not writer.is_closing():
                writer.close()


    async def wait_closed(self):
        if self._conn:
            _, writer = self._conn
            if writer.is_closing():
                await writer.wait_closed()
                self._conn = None

    async def call(self, method, *args, **kwargs):
        request = Request(method, None, args, kwargs)
        response = await self._remote_call(request)
        if response.exception:
            raise response.exception
        return response.result

    def sync_call(self, method, *args, **kwargs):
        result = sync_await(self.call(method, *args, **kwargs))
        self.close()
        return result

    async def _remote_call(self, request, response_Q=None, exception_Q=None):
        reader, writer = await self._connect()
        if request.id is None:
            request.id = self._req_num
            self._req_num += 1
        response = Response.for_request(request)

        #  Send rpc request
        request_bytes = pickle.dumps(request)
        writer.write(request_bytes)
        writer.write(SENTINEL)
        self.on_rpc_call(request)
        #  Receive rpc response and close connection
        try:
            response_bytes = await asyncio.wait_for(reader.readuntil(SENTINEL), timeout=self._network_timeout)
        except asyncio.TimeoutError:
            err = RPCConnectionTimeoutError(f"Timeout waiting for server")
            if exception_Q:
                await exception_Q.put(err)
            else:
                raise err
            response.exception = err
            return response
        #  finally:
        #      writer.write_eof()
        #      writer.close()

        #  Parse response
        response = pickle.loads(response_bytes.strip(SENTINEL))
        self.on_rpc_returned(response)
        if response_Q:
            await response_Q.put(response)
        return response

    def on_rpc_call(self, request):
        req_desc = f"<{request.id}, {request.method}>"
        log("request", f"Sent request {req_desc}")
        #  super().on_rpc_call(request)

    def on_rpc_returned(self, response):
        #  super().on_rpc_returned(response)
        status = response.status if response.id not in self.__received else "discard"
        #  res_desc = f"<{self._tag}, {response.server._tag}, {response.id}, {response.status}> {response.result}"
        res_desc = f"<{response.id}, {response.status}> {response.result}"
        log(status, f"Received {res_desc}")
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

    #  def __getstate__(self):
    #      return {"_tag": self._tag, "_host": self._host, "_port": self._port}

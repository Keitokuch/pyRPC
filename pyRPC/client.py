import asyncio
import pickle

from .server import RPCServer
from .util import log
from .exceptions import *
from .common import *
from .framework_common import *


RPC_TIMEOUT=3
NW_TIMEOUT=3


class RPCClient():

    def __init__(self, tag=None, loop=None, rpc_timeout=RPC_TIMEOUT, network_timeout=NW_TIMEOUT, **kwargs):
        self._tag = tag or "RPCClient"
        self._req_num = 0
        self._rpc_timeout = rpc_timeout
        self._network_timeout = network_timeout
        self.__received = set()
        self._loop = loop or get_event_loop()

    async def call(self, fname, hostport, *args, **kwargs):
        try:
            host, port = hostport.split(':')
        except ValueError:
            raise RPCError("RPCClient.call: port not provided in hostport {}".format(hostport))
        #  if not port:
        #      raise RPCError("RPCClient.call: port must be provided for rpc call")
        request = Request(fname, self, RPCServer(host=host, port=port), None, args, kwargs)
        response = await self._remote_call(request)
        if response.exception:
            raise response.exception
        return response.result

    def sync_call(self, fname, hostport, *args, **kwargs):
        return sync_await(self.call(fname, hostport, *args, **kwargs))

    async def _remote_call(self, request, response_Q=None, exception_Q=None):
        if request.id is None:
            request.id = self._req_num
            self._req_num += 1
        response = Response.for_request(request)
        # Generate connection with node
        server = request.server
        try:
            reader, writer = await asyncio.open_connection(
                host=server._host, port=server._port)
        except Exception as e:
            err = RPCConnectionError(
                f"Failed connecting to {server}: {e}.", node=server)
            if exception_Q:
                await exception_Q.put(err)
            else:
                raise err from e
            response.exception = err
            return response

        #  Send rpc request
        request_bytes = pickle.dumps(request)
        writer.write(request_bytes)
        writer.write(b"\r\n")
        self.on_rpc_call(request)
        #  Receive rpc response and close connection
        try:
            response_bytes = await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=self._network_timeout)
        except asyncio.TimeoutError:
            err = RPCConnectionTimeoutError(
                f"Timeout waiting for server {server._tag}.", node=server)
            if exception_Q:
                await exception_Q.put(err)
            else:
                raise err
            response.exception = err
            return response
        finally:
            writer.write_eof()
            writer.close()

        #  Parse response
        response = pickle.loads(response_bytes.strip(b"\r\n"))
        #  if response.exception:
        #      #  Remote function returned error
        #      if exception_Q:
        #          await exception_Q.put(response.exception)
        #      else:
        #          raise result
        #  Log
        self.on_rpc_returned(response)
        #  Return response
        if response_Q:
            await response_Q.put(response)
        await writer.wait_closed()
        return response

    def on_rpc_call(self, request):
        req_desc = f"<{self._tag}, {request.server._tag}, {request.id}, {request.rpc_name}>"
        log("request", f"Sent request {req_desc}")
        #  super().on_rpc_call(request)

    def on_rpc_returned(self, response):
        #  super().on_rpc_returned(response)
        status = response.status if response.id not in self.__received else "discard"
        res_desc = f"<{self._tag}, {response.server._tag}, {response.id}, {response.status}> {response.result}"
        log(status, f"Received {res_desc}")
        self.__received.add(response.id)

    #  Set timeouts
    def set_rpc_timeout(self, timeout):
        self.__rpc_timeout = timeout
        return self

    def set_network_timeout(self, timeout):
        self._network_timeout = timeout
        return self

    def __reduce__(self):
        return (RPCClient, (self._tag,))

    #  def __getstate__(self):
    #      return {"_tag": self._tag, "_host": self._host, "_port": self._port}

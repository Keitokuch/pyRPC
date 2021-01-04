import asyncio
from asyncio.streams import StreamReader, StreamWriter
import pickle
import socket
import logging
import traceback
import sys

from .framework_common import *
from .common import *
from .util import *
from .exceptions import *


LOGGER = logging.getLogger("RPCServer")
DEFAULT_HOST = ""


class RPCServer():
    def __init__(self, tag=None, host=None, port=None, loop=None, listen=False, debug=False, **kwargs):
        self._tag = tag or "RPCServer"
        self._host = host
        self._port = port
        self._loop = loop or get_event_loop()
        self.__debug = debug
        self.__listener = None
        self.__rpcs = {}

        if listen:
            self._listen()

    def _set_debug(self, debug):
        self.__debug = debug

    def _collect_rpcs(self):
        for objname in dir(self.__class__):
            func = getattr(self.__class__, objname)
            if is_rpc(func):
                self.__rpcs[objname] = func
        return self

    def rpc(self, func):
        self.__rpcs[func.__name__] = func

    def run(self, host=DEFAULT_HOST, port=None):
        self._listen(host, port)
        try:
            self._loop.run_forever()
        except KeyboardInterrupt:
            print("Stopping.")

    def _listen(self, host=None, port=None):
        self._collect_rpcs()
        # If we are changing port, close old listen socket
        if self.__listener:
            self.__listener.close()
        self.__listener = self._loop.run_until_complete(asyncio.start_server(self._serve_remote_call, host=host, port=port, family=socket.AF_INET))
        self.__sock = self.__listener.sockets[0]
        if not self._host:
            self._host = self.__sock.getsockname()[0]
        if not self._port:
            self._port = self.__sock.getsockname()[1]
        print(f"{self._tag} Listening: {self.__sock.getsockname()}", flush=True)
        return self

    def on_rpc_called(self, request):
        req_desc = f"<{request.id}, {request.method}>"
        log(request.method, f"Received request {req_desc}")

    def on_rpc_return(self, response):
        pass

    async def _serve_remote_call(self, reader: StreamReader, writer: StreamWriter):
        """ callback function of asyncio tcp server """
        try:
            #  sock = writer.get_extra_info("socket")
            while not reader.at_eof():
                #  Read and parse rpc packet
                request_bytes = await reader.readuntil(SENTINEL)
                request = pickle.loads(request_bytes.strip(SENTINEL))
                #  Serve request
                if request.method in self.__rpcs:
                    self.on_rpc_called(request)
                    response = await self._process_request(request)
                    self.on_rpc_return(response)
                else:
                    #  print("Service", request.method, "not found")
                    err = RPCNotFoundError(f"RPC <{request.method}> not found in {self._tag}")
                    response = Response.for_request(request)
                    response.exception = err
                    print(err)
                response_bytes = pickle.dumps(response)
                writer.write(response_bytes)
                writer.write(SENTINEL)
            # EOF
            #  log("connection", "Connection dropped")
        except asyncio.CancelledError:
            writer.write_eof()
        except asyncio.IncompleteReadError:
            pass
        except ConnectionResetError as e:
            log("WARN", f"Connection dropped")
        except Exception as e:
            print(request_bytes, e)

    async def _process_request(self, request):
        #  Call service func/coro and send result back
        method = request.method
        rpc = self.__rpcs[method]
        status = "success"
        result = None
        exception = None
        try:
            if is_service_rpc(rpc):
                result = rpc(self, *request.args, **request.kwargs)
            else:
                result = rpc(*request.args, **request.kwargs)
            while asyncio.iscoroutine(result):
                result = await result
        except Exception as e:
            #  Catch any exception raised in rpc function
            _name = self.__class__.__name__
            _full_rpc = f"{_name}.{method}"
            err_header = f"Exception in rpc \"{_full_rpc}\":"
            LOGGER.exception(err_header)
            # Send err msg to client in exception
            exc = traceback.format_exception(*sys.exc_info())
            *elines, etype = exc[2:]
            err_msg = ['\n'+err_header+'\n']
            if self.__debug:
                err_msg += elines
            err_msg += [etype]
            err_msg = "".join(err_msg)
            rpc_exc = RPCRuntimeException(err_msg, original=e)
            result = rpc_exc
            exception = rpc_exc
            status = "exception"
        response = Response(
            request.method, request.id, status, result, exception)
        return response

    def __reduce__(self):
        return (RPCServer, (self._tag, self._host, self._port))

    def __getstate__(self):
        return {"_tag": self._tag, "_host": self._host, "_port": self._port}
    
    def __repr__(self):
        return "{}({}, {})".format(self._tag, self._host, self._port)

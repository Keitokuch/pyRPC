import asyncio
import socket
import logging
import traceback
import sys
from types import MethodType

from . import config

from .framework_common import *
from .common import *
from .utils import *
from .exceptions import *


__all__ = ['RPCServer']


LOGGER = logging.getLogger("RPCServer")


class RPCServer():
    def __init__(self, tag=None, host=None, port=None, loop=None, protocol=None, debug=None, **kwargs):
        self._tag = tag or "RPCServer"
        self._host = host
        self._port = port
        self._loop = loop
        self._listener = None
        self._rpcs = {}
        self._rpcs_collected = False
        self._debug = debug if debug is not None else config.debug
        self._to_schedule = []
        self._tasks = []
        protocol = protocol or config.protocol
        self.__protocol_factory = protocol['server'] if isinstance(protocol, dict) else protocol
        _ = kwargs

    def _set_debug(self, debug):
        self._debug = debug

    def _collect_rpcs(self):
        if not self._rpcs_collected:
            for objname in dir(self.__class__):
                func = getattr(self.__class__, objname)
                if is_rpc(func):
                    self._rpcs[objname] = MethodType(func, self)
            self._rpcs_collected = True
        return self

    def rpc(self, func):
        self._rpcs[func.__name__] = func

    """ Async App """

    def create_task(self, coro):
        if self._loop:
            self._tasks.append(self._loop.create_task(coro))
        else:
            self._to_schedule.append(coro)

    def run(self, host=None, port=None, debug=None):
        self._run(host, port, debug, False)

    def run_in_thread(self, host=None, port=None, debug=None):
        self._run(host, port, debug, True)
        return self

    def _run(self, host, port, debug, to_thread):
        if debug is not None: self._debug = debug
        self._host = host or self._host
        self._port = port or self._port

        if to_thread:
            self._loop = start_loop_in_thread(self._loop, False, self._debug)
            asyncio.run_coroutine_threadsafe(self._init_task(), self._loop)
        else:
            self._loop = self._loop or asyncio.new_event_loop()
            self._loop.set_debug(self._debug)
            asyncio.set_event_loop(self._loop)
            self._loop.create_task(self._init_task())
            try:
                self._loop.run_forever()
            except (KeyboardInterrupt, SystemExit, NodeStopException):
                self._clean_up()
                print()
                print('Stopping...')

    async def _init_task(self):
        await self._listen(self._host, self._port)
        for coro in self._to_schedule:
            self._tasks.append(self._loop.create_task(coro))
        for objname in dir(self.__class__):
            func = getattr(self.__class__, objname)
            if is_task(func):
                self._tasks.append(self._loop.create_task(func(self)))

    def stop(self):
        self._stop()

    def _stop(self):
        if not self._loop.is_running():
            return
        self._clean_up()
        self._loop.call_soon_threadsafe(self._loop.stop)
        print()
        print('Stopping..')

    def _clean_up(self):
        if self._listener:
            self._listener.close()
        for task in self._tasks:
            if not task.cancelled() or not task.done():
                task.cancel()

    """ Serving """

    async def _listen(self, host=None, port=None):
        self._collect_rpcs()
        # If we are changing port, close old listen socket
        if self._listener:
            self._listener.close()
        host = host or self._host
        port = port or self._port
        if port is None:
            port = 0
        #  self._listener = await asyncio.start_server(self._serve_remote_call, host=host, port=port, family=socket.AF_INET)
        self._listener = await self._loop.create_server(
            lambda: self.__protocol_factory(self), host=host, port=port, family=socket.AF_INET)
        self.__sock = self._listener.sockets[0]
        if not self._host:
            self._host = self.__sock.getsockname()[0]
        if not self._port:
            self._port = self.__sock.getsockname()[1]
        print(f"{self._tag} Listening: {self.__sock.getsockname()}", flush=True)

    async def _handle_request(self, request: Request):
        response = Response.for_request(request)
        try:
            if request.method in self._rpcs:
                self._on_rpc_called(request)
                response = await self._process_request(request)
                self._on_rpc_return(response)
            else:
                err = RPCNotFoundError(f"RPC <{request.method}> not found in {self._tag}")
                response.error = err
                #  print(err)
        except asyncio.CancelledError:
            response.status = 'error'
            response.error = RPCError('RPCServer closed')
        finally:
            return response

    def _on_rpc_called(self, request):
        log(request.method, f"Received request {request}")
        return

    def _on_rpc_return(self, response):
        log(response.status, f"Sent response {response}")
        return

    async def _process_request(self, request):
        #  Call service func/coro and send result back
        method = request.method
        rpc = self._rpcs[method]
        status = "success"
        result = None
        exception = None
        try:
            #  if asyncio.iscoroutinefunction(rpc):
            #      result = await rpc(*request.args, **request.kwargs)
            #  else:
            #      result = rpc(*request.args, **request.kwargs)
            result = await self._loop.run_in_executor(
                None, rpc, *request.args, **request.kwargs)
        except Exception as e:
            #  Catch any exception raised in rpc function
            _name = self.__class__.__name__
            _full_rpc = f"{_name}.{method}"
            err_header = f"Exception in rpc \"{_full_rpc}\":"
            # Send err msg to client in exception
            exc = traceback.format_exception(*sys.exc_info())
            *elines, etype = exc[2:]
            err_msg = ['\n'+err_header+'\n']
            if self._debug:
                err_msg += elines
            else:
                LOGGER.exception(err_header)
            err_msg += [etype]
            err_msg = "".join(err_msg)
            rpc_exc = RPCRuntimeException(err_msg, original=e)
            result = None
            exception = rpc_exc
            status = "exception"
        response = Response(
            request.method, request.id, status, result, exception)
        return response

    def __reduce__(self):
        return (RPCServer, (self._tag, self._host, self._port))

    def __getstate__(self):
        return {"_tag": self._tag, "_host": self._host, "_port": self._port}

    def __str__(self):
        return "{}({}, {})".format(self._tag, self._host, self._port)

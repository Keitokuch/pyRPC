import asyncio
from inspect import isfunction
from types import MethodType
from functools import wraps
from typing import Any, Callable, Coroutine, List, Optional

from aioconsole import ainput

from . import config
from .client import RPCClient
from .server import RPCServer
from .common import *
from .exceptions import *
from .framework_common import *
from .utils import *


__all__ = ['Service']


class Service(RPCClient, RPCServer):
    def __init__(self, tag=None, loop=None, blocking=None, debug=None, **kwargs):
        tag = tag or self.__default_tag()
        RPCClient.__init__(self, loop=loop, debug=debug, **kwargs)
        RPCServer.__init__(self, tag=tag, loop=loop, debug=debug, **kwargs)
        self._tag = tag
        self._loop = loop
        self._debug = debug if debug is not None else config.debug
        self._nodes = {}
        self.__type = LOCAL
        self.__name = self.__class__.__name__
        self._blocking = blocking if blocking is not None else config.blocking
        self._service_rpcs = {}
        self._anon_nodes_cnt = 0
        self._kwargs = kwargs

    """ Service related """

    def __default_tag(self):
        return self.__class__.__name__ + "_Service"

    @property
    def service_type(self):
        return self.__type

    def _new_anon_tag(self):
        self._anon_nodes_cnt += 1
        return f"anonymous_node_{self._anon_nodes_cnt}"

    """ Override RPCServer """

    def _collect_rpcs(self):
        for objname in dir(self.__class__):
            func = getattr(self.__class__, objname)
            if is_service_rpc(func):
                self._service_rpcs[objname] = func
        return super()._collect_rpcs()

    def _run(self, host, port, debug, to_thread):
        if self.__type != LOCAL:
            print(f"Service of {self.__type} type can't run.")
            return
        self.__type = ONLINE
        RPCServer._run(self, host, port, debug, to_thread)

    async def _init(self):
        init = self.init()
        if asyncio.iscoroutine(init):
            await init

    async def init(self):
        pass

    def _stop(self):
        if self.__type != ONLINE:
            return
        super()._stop()

    async def _clean_up(self):
        cleanup = self.clean_up()
        if asyncio.iscoroutine(cleanup):
            await cleanup
        self.__type = LOCAL
        await super()._clean_up()

    def _on_rpc_called(self, request):
        if request.method in self._service_rpcs:
            self.on_rpc_called(request)
        super()._on_rpc_called(request)

    def _on_rpc_return(self, response):
        super()._on_rpc_return(response)
        if response.method in self._service_rpcs:
            self.on_rpc_return(response)

    """ Override RPCClient """

    def call(self, method, *args, **kwargs):
        if self.__type in [REMOTE, REMOTE_NODE]:
            rpc = self._rpcs[method]
            return rpc(*args, **kwargs)
        raise RuntimeError(f'Cannot make rpc calls with {self.__type} service')

    def connect(self, hostport=None, host=None, port=None):
        return self.at(hostport, host, port)

    def _on_rpc_call(self, request):
        if request.method in self._service_rpcs:
            self.on_rpc_call(request)
        super()._on_rpc_call(request)

    def _on_rpc_returned(self, response):
        super()._on_rpc_returned(response)
        if response.method in self._service_rpcs:
            self.on_rpc_returned(response)

    """ Async App """

    def console(self, main: Callable[[List[str]], Any]=None):
        if main is not None and isfunction(main):
            setattr(self, 'main', main)
        self._loop.create_task(self._console())
        return self

    async def _console(self):
        while True:
            try:
                cmdline = await ainput(">>> ")
                args = cmdline.split(" ")
                if not args:
                    continue
                ret = self.main(args)
                if asyncio.iscoroutine(ret):
                    ret = await ret
                if ret is not None:
                    print("<<<", ret)
                await asyncio.sleep(1/50)
            except EOFError as e:
                raise NodeStopException from e

    """ Service RPC """

    def _make_rpc(self, func, call_routine):
        method = func.__name__

        @wraps(func)
        async def async_rpc(_, *args, **kwargs):
            ret = await call_routine(method, args, kwargs)
            return ret

        @wraps(func)
        def sync_rpc(self, *args, **kwargs):
            ret = sync_await(call_routine(method, args, kwargs), self._loop)
            return ret

        if self._blocking or is_sync(func):
            return sync_rpc
        return async_rpc

    def _make_rpcs(self, call_routine):
        for fname in dir(self.__class__):
            func = getattr(self.__class__, fname)
            if is_rpc(func):
                rpc = self._make_rpc(func, call_routine)
                rpc = MethodType(rpc, self)
                setattr(self, fname, rpc)
                self._rpcs[fname] = rpc
                if is_service_rpc(func):
                    self._service_rpcs[fname] = rpc

    # Service Node

    def at(self, hostport=None, tag=None, host=None, port=None):
        if self.__type != LOCAL:
            return
        if hostport:
            host, port = load_addr(hostport)
        self._host = host
        self._port = port
        if not self._port:
            raise RPCError("port must be provided to locate service")
        self._tag = str(tag) or self._tag
        return self._make_remote_node()

    def _make_remote_node(self):
        if self.__type != LOCAL:
            return
        if self._blocking:
            self._loop = start_loop_in_thread(self._loop, daemon=True, debug=self._debug)
        self._make_rpcs(self._node_call)
        self.__type = REMOTE_NODE
        return self

    async def _node_call(self, method, args, kwargs):
        request = Request(method, self._req_num, args, kwargs)
        self._req_num += 1
        call = self._remote_call(request)
        try:
            ret = await asyncio.wait_for(call, timeout=self._rpc_timeout)
        except asyncio.TimeoutError:
            raise RPCTimeoutError("RPC timeout")
        if ret.error:
            raise ret.error
        return ret.result

    # Node group

    def remote(self):
        if self.__type != LOCAL:
            print("Error: can't make {self} remote")
            return
        return self._make_remote_service()

    def _make_remote_service(self):
        if self.__type != LOCAL:
            return
        if self._blocking:
            self._loop = start_loop_in_thread(self._loop, daemon=True, debug=self._debug)
        self._make_rpcs(self._group_call)
        self.__type = REMOTE
        return self

    async def _group_call(self, method, args, kwargs):
        if not self.nodes:
            return []
        results = {}
        tasks = []
        contexts = {}
        for _, node in self.nodes.items():
            request = Request(method, self._req_num, args, kwargs)
            self._req_num += 1
            task = asyncio.create_task(node._remote_call(request))
            task.set_name(node._tag)
            tasks.append(task)
            contexts[node._tag] = (node, request)
        done, pending = await asyncio.wait(tasks, timeout=self._rpc_timeout)
        for call in pending:
            call.cancel()
            node, request = contexts[call.get_name()]
            log('timeout', f'RPC call {request} to {node} timed out after {self._rpc_timeout} seconds.')
        for call in done:
            if call.exception():
                excp = call.exception()
                node, request = contexts[call.get_name()]
                log('error', f'RPC call {request} to {node} failed: {excp}')
            else:
                results[call.get_name()] = call.result().result
        return results

    @property
    def nodes(self) -> dict:
        if self.__type != REMOTE:
            #  print("Error: can only get nodes of a remote service")
            return {}
        return self._nodes

    def add_node(self, node_or_hostport=None, host=None, port=None, tag=None, verify=False):
        if self.__type == LOCAL:
            self._make_remote_service()
        if self.__type != REMOTE:
            print("Error: Can only add node to remote service")
            return
        if isinstance(node_or_hostport, Service):
            node = node_or_hostport
            if isinstance(node, self.__class__) and node.__type == REMOTE_NODE:
                if not node._tag:
                    node._tag = self._new_anon_tag()
                new_node = node
            else:
                print(f"Error: node {node} can't be added to {self}")
                return
        else:
            hostport = node_or_hostport
            if not tag:
                tag = self._new_anon_tag()
            new_node = self.__class__()
            Service.__init__(new_node, blocking=self._blocking, debug=self._debug)
            new_node.at(hostport, tag=tag, port=port, host=host)
        self._nodes[new_node._tag] = new_node
        if verify:
            try:
                tag = new_node._verify(new_node.__class__)
                new_node._tag = tag
            except (RPCError, NodeException) as e:
                self.remove_node(new_node)
                log("error",
                    "Failed to add node because node can't be verified: " + str(e))
                return
        return new_node

    @sync
    @rpc__
    def _verify(self, service_class):
        if service_class.__name__ != self.__class__.__name__:
            raise NodeException("Invalid service type requested")
        return self._tag

    def get_node(self, tag):
        if self.__type != REMOTE:
            print("Error: Can only get node of a remote service")
            return
        return self._nodes.get(tag)

    def remove_node(self, node_tag):
        if self.__type != REMOTE:
            print("Error: Can only remove node from a remote service")
            return
        if isinstance(node_tag, Service):
            tag = node_tag._tag
        else:
            tag = node_tag
        try:
            return self._nodes.pop(tag)
        except KeyError:
            print(f"Error: remove_node: node {tag} doesn't exist for service {self}")
            pass

    """ Template Methods """

    def clean_up(self):
        pass

    def main(self, args):
        return "main() not implemented."

    def on_rpc_called(self, request: Request):
        pass

    def on_rpc_return(self, response: Response):
        pass

    def on_rpc_call(self, request: Request):
        pass

    def on_rpc_returned(self, response: Response):
        pass

    """ Misc """

    @rpc__
    def heartbeat(self):
        pass

    def __getstate__(self):
        return {"_tag": self._tag, "_host": self._host, "_port": self._port}

    def __str__(self):
        if self.__type == LOCAL:
            return f"Local { self.__name } service <{ self._tag }>"
        elif self.__type == REMOTE:
            return f"Remote { self.__name } service <{ self._tag }>"
        elif self.__type == ONLINE:
            return f"Online { self.__name } Node <{RPCServer.__str__(self)}>"
        elif self.__type == REMOTE_NODE:
            return f"{self._tag}({self._host}, {self._port})"
        else:
            return f"{ self.__name } Service of invalid type: { self.__type }"

    def __repr__(self) -> str:
        if self.__type == ONLINE:
            return f"<{self.__name} Service type={self.__type} debug={self._debug} host={self._host} port={self._port}>"
        elif self.__type == REMOTE_NODE:
            return f"<{self.__name} Service type={self.__type} blocking={self._blocking} debug={self._debug} host={self._host} port={self._port}>"
        else:
            return f"<{self.__name} Service type={self.__type} blocking={self._blocking} debug={self._debug}>"

    # For comparing
    def __eq__(self, other):
        if isinstance(other, Service):
            return self._tag == other._tag
        return False

    def __hash__(self):
        return hash((self._tag, self._host, self._port))

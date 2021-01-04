import asyncio
import pickle
import sys
from inspect import isfunction, ismethod
from types import MethodType
from functools import wraps

from aioconsole import ainput

from .client import RPCClient
from .server import RPCServer
from .common import *
from .exceptions import *
from .framework_common import *
from .util import *


__all__ = ['Service', 'ReplicatedService']


def make_rpc(func):
    method = func.__name__

    @wraps(func)
    async def rpc(self, *args, **kwargs):
        exception_Q = asyncio.Queue(1)
        request = Request(method, None, args=args, kwargs=kwargs)
        call = self._remote_call(request, exception_Q=exception_Q)
        try:
            ret = await asyncio.wait_for(call, timeout=self._rpc_timeout)
        except asyncio.TimeoutError:
            try:
                err = exception_Q.get_nowait()
                raise err
            except asyncio.QueueEmpty:
                pass
            raise RPCTimeoutError(
                f"RPC '{method}' to {self} timeout after {self._rpc_timeout}s")
        if ret.exception:
            raise ret.exception
        return ret.result
    if is_sync(func):
        rpc = make_sync(rpc)
    rpc._rpc = True
    return rpc

def make_replica_rpc(func):
    method = func.__name__

    @wraps(func)
    async def rpc(self, *args, **kwargs):
        exception_Q = asyncio.Queue(1)
        call = self._replica_remote_call(method, args, kwargs, exception_Q=exception_Q)
        try:
            ret = await asyncio.wait_for(call, timeout=self._rpc_timeout)
        except asyncio.TimeoutError as e:
            try:
                while True:
                    err = exception_Q.get_nowait()
                    print(err)
            except asyncio.QueueEmpty:
                pass
            raise RPCTimeoutError(
                f"Replicated RPC '{method}' timeout after {self._rpc_timeout}s") from e
        if ret.exception:
            raise ret.exception
        return ret.result
    rpc._rpc = True
    return rpc


def make_group_rpc(func):
    method = func.__name__

    @wraps(func)
    async def rpc(self, *args, **kwargs):
        exception_Q = asyncio.Queue(1)
        call = self._group_remote_call(method, args, kwargs, exception_Q=exception_Q)
        try:
            ret = await asyncio.wait_for(call, timeout=self._rpc_timeout)
        except asyncio.TimeoutError as e:
            try:
                while True:
                    err = exception_Q.get_nowait()
                    raise err
            except asyncio.QueueEmpty:
                pass
            raise RPCTimeoutError(
                f"Group RPC '{method}' timeout after {self._rpc_timeout}s") from e
        try:
            while True:
                err = exception_Q.get_nowait()
                print(err)
        except asyncio.QueueEmpty:
            pass
        for _, res in ret.items():
            if res.exception:
                print(res.exception)
        return {tag: res.result for tag, res in ret.items()}
    rpc._rpc = True
    return rpc


class Service(RPCClient, RPCServer):
    def __init__(self, tag=None, host=None, port=None, config=None, loop=None, debug=False, remote_node=False, **kwargs):
        if config:
            tag = config["tag"]
            host = config["host"]
            port = config["port"]
        tag = tag or self.__default_tag()
        loop = loop or asyncio.get_event_loop()
        self.__debug = debug
        super(Service, self).__init__(tag=tag, loop=loop, **kwargs)
        super(RPCClient, self).__init__(tag=tag, host=host, port=port, loop=loop, debug=debug, **kwargs)
        self._tag = tag
        self._loop = loop
        self._tasks = []
        self.__type = LOCAL
        self.__nodes = {}
        self.__name = self.__class__.__name__
        self.__rpc_maker = make_group_rpc
        self._service_rpcs = {}
        self._hash = hash(self.__class__)
        self._anon_nodes_cnt = 0
        if remote_node:
            self._make_remote_node()

    def __default_tag(self):
        return self.__class__.__name__ + "_Service"

    @property
    def service_type(self):
        return self.__type

    def run(self, host=None, port=None):
        if self.__type != LOCAL:
            print(f"Service of {self.__type} type can't run.")
            sys.exit(1)
        self._loop.set_debug(self.__debug)
        self._listen(host, port)
        for objname in dir(self.__class__):
            func = getattr(self.__class__, objname)
            if is_task(func):
                self._tasks.append(self._loop.create_task(func(self)))
        try:
            self._loop.run_forever()
        except (KeyboardInterrupt, NodeStopException):
            self.__stop()

    def __stop(self):
        print()
        for task in self._tasks:
            task.cancel()
        #  self._loop.stop()
        self.__clean_up()
        print("Stopping.")

    def __clean_up(self):
        cleanup = self.clean_up()
        if asyncio.iscoroutine(cleanup):
            self._loop.run_until_complete(cleanup)

    def clean_up(self):
        pass

    #  Create an async task to run later
    def create_task(self, coro):
        self.__create_task(coro)

    def __create_task(self, coro):
        if asyncio.iscoroutine(coro):
            self._tasks.append(self._loop.create_task(coro))
        else:
            self._loop.call_soon(coro, ())

    def _make_remote_node(self):
        if self.__type != LOCAL:
            return
        for fname in dir(self.__class__):
            func = getattr(self.__class__, fname)
            if is_rpc(func):
                rpc = make_rpc(func)
                setattr(self, fname, MethodType(rpc, self))
        self.__type = REMOTE_NODE
        return self

    def _make_remote_service(self):
        if self.__type != LOCAL:
            return
        for fname in dir(self.__class__):
            func = getattr(self.__class__, fname)
            if is_rpc(func):
                rpc = self.__rpc_maker(func)
                if is_sync(func):
                    rpc = make_sync(rpc)
                setattr(self, fname, MethodType(rpc, self))
        self.__type = REMOTE
        return self

    def _collect_rpcs(self):
        for objname in dir(self.__class__):
            func = getattr(self.__class__, objname)
            if is_service_rpc(func):
                self._service_rpcs[objname] = func
        return super()._collect_rpcs()

    #  Return first response
    async def _replica_remote_call(self, method, args, kwargs, exception_Q=None):
        responses = asyncio.Queue()
        if not self.nodes:
            return Response(method, self, None, -1, result="No node available for requested service.")
        for _, node in self.nodes.items():
            request = Request(method, self._req_num, args, kwargs)
            asyncio.create_task(node._remote_call(
                request, response_Q=responses, exception_Q=exception_Q))
        self._req_num += 1
        res = None
        for _ in range(len(self.nodes)):
            res = await responses.get()
            if res.exception:
                continue
            else:
                break
        return res

    #  Wait for all responses
    async def _group_remote_call(self, method, args, kwargs, exception_Q=None):
        #  responses = asyncio.Queue()
        if not self.nodes:
            return []
        results = {}
        tasks = []
        for _, node in self.nodes.items():
            request = Request(method, self._req_num, args, kwargs)
            self._req_num += 1
            task = (asyncio.create_task(node._remote_call(
                request, response_Q=None, exception_Q=exception_Q)))
            task.set_name(node._tag)
            tasks.append(task)
        for task in tasks:
            await task
            results[task.get_name()] = task.result()
        return results

    def remote(self):
        if self.__type != LOCAL:
            print("Error: can't make {self} remote")
            return
        return self._make_remote_service()

    @property
    def nodes(self) -> dict:
        if self.__type != REMOTE:
            #  print("Error: can only get nodes of a remote service")
            return {}
        return self.__nodes

    def add_node(self, node=None, host=None, port=None, tag=None, verify=False):
        if self.__type == LOCAL:
            self._make_remote_service()
        if self.__type != REMOTE:
            print("Error: Can only add node to remote service")
            return
        if isinstance(node, self.__class__) and node.__type == REMOTE_NODE:
            new_node = node
        else:
            if not tag:
                tag = self._new_anon_tag()
            new_node = self.__class__().at(node, tag=tag, port=port, host=host)
        self.__nodes[new_node._tag] = new_node
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
        return self.__nodes.get(tag)

    def remove_node(self, node_tag):
        if self.__type != REMOTE:
            print("Error: Can only remove node from a remote service")
            return
        if isinstance(node_tag, Service):
            tag = node_tag._tag
        else:
            tag = node_tag
        try:
            return self.__nodes.pop(tag)
        except KeyError:
            print(f"Error: remove_node: node {tag} doesn't exist for service {self}")
            pass

    def at(self, hostport: str=None, host: str=None, port=None, tag: str=None):
        if hostport:
            host, port = load_addr(hostport)
        if not port:
            raise RPCError("port must be provided to locate service")
        return self.__at(port=port, host=host, tag=tag)


    def __at(self, tag, port, host=None):
        self._make_remote_node()
        self._port = port
        self._tag = tag
        if host:
            self._host = host
        return self

    def _new_anon_tag(self):
        self._anon_nodes_cnt += 1
        return f"_anonymous_node_{self._anon_nodes_cnt}"

    @rpc__
    def heartbeat(self):
        pass

    def console(self, main=None):
        if main and isfunction(main):
            self.main = main
        self._loop.create_task(self.__console())
        return self

    async def __console(self):
        while True:
            try:
                cmdline = await ainput(">>> ")
                args = cmdline.split(" ")
                if not args:
                    continue
                main = self.main
                #  if ismethod(main):
                #      ret = main(self, args)
                #  else:
                ret = main(args)
                if asyncio.iscoroutine(ret):
                    ret = await ret
                if ret is not None:
                    print("<<<", ret)
                await asyncio.sleep(1/50)
            except EOFError as e:
                raise NodeStopException from e

    def main(self, args):
        print("main() not implemented.")

    #  For pickling
    def __getstate__(self):
        return {"_tag": self._tag, "_host": self._host, "_port": self._port}

    def __reduce__(self):
        return (Service, (self._tag, self._host, self._port))

    #  For printing
    def __repr__(self):
        if self.__type == LOCAL:
            return f"Local { self.__name } service <{ self._tag }>"
        elif self.__type == REMOTE:
            return f"Remote { self.__name } service <{ self._tag }>"
        elif self.__type == ONLINE:
            return f"Active { self.__name } Node <{ self._tag }({ self._host }, { self._port })"
        elif self.__type == REMOTE_NODE:
            return f"{self._tag}({self._host}, {self._port})"
        else:
            return f"Service { self.__name } of invalid type: { self.__type }"

    # For comparing
    def __eq__(self, other):
        if isinstance(other, RPCNode):
            return self._tag == other._tag
        return False

    def __hash__(self):
        return hash((self._tag, self._host, self._port))

    def loop_exception_handler(self, loop, context):
        try:
            exception = context["exception"]
            if isinstance(exception, NodeStopException):
                self.__stop()
                return
            else:
                print(exception)
            #  if isinstance(exception, RPCError):
            #      return
        except:
            pass
        #  loop.default_exception_handler(context)

    @classmethod
    def from_dict(cls, dict):
        node = cls(tag=dict["tag"], host=dict["host"], port=dict["port"])
        return node


class ReplicatedService(Service):
    def __init__(self, tag=None, host=None, port=None, config=None, loop=None,
                 cp_int=5, replicated=True, **kwargs):
        super().__init__(tag, host, port, config, loop, **kwargs)
        self.is_replica = replicated
        self.is_primary = False
        self.cp_int = cp_int
        self.cp_count = 0
        self._ready = False
        self._state_ready = False
        self.request_queue = []
        self._Service__rpc_maker = make_replica_rpc if self.is_replica else make_group_rpc

    def run(self):
        type(self).backups = ReplicatedService(replicated=False)
        super().run()

    def set_checkpoint_interval(self, interval):
        self.cp_int = interval

    @rpc__
    async def set_primary(self, will_be_primary):
        #  if not self._state_ready:
        #      return False
        if self.is_primary and not will_be_primary:
            self.checkpointing_task.cancel()
        if not self.is_primary and will_be_primary:
            await self.set_ready()
            self.checkpointing_task = self.create_task(self._checkpointing())
        self.is_primary = will_be_primary
        return True

    @rpc__
    async def set_ready(self):
        if self._ready:
            return
        print(len(self.request_queue))
        for request, writer in self.request_queue:
            log("apply_log", f"{request.method} {request.args}")
            response = await self.process_request(request)
            response_bytes = pickle.dumps(response)
            if writer is not None:
                writer.write(response_bytes)
                writer.write(SENTINEL)
        self.request_queue = []
        self._ready = True

    @rpc__
    async def state_ready(self):
        self._state_ready = True

    @rpc__
    def quiesce(self):
        self._ready = False

    @rpc__
    def add_backup(self, backup_node):
        if self.is_primary:
            # backup_node.put_checkpoint(self.is_primary.get_checkpoint())
            # for i in
            backup_node = self.backups.add_node(backup_node)
            # print(self.backups)

    @rpc__
    def remove_backup(self, backup_node):
        if self.is_primary:
            self.backups.remove_node(backup_node)

    async def _checkpointing(self):
        while True:
            if self.backups.nodes:
                cp = self.get_checkpoint()
                if cp != None:
                    try:
                        await self.backups.checkpoint(cp, self.cp_count)
                        self.cp_count += 1
                    except RPCError as e:
                        print(e)
            await asyncio.sleep(self.cp_int)

    def on_service_called(self, request):
        if self.is_primary and self._ready:
            if request.method in self.__services:
                self.create_task(self.backups.append_log(request))
        super().on_service_called(request)

    @rpc__
    def append_log(self, request):
        self.request_queue.append((request, None))

    @rpc__
    async def restore_peer(self, peer_node):
        self._ready = False
        for request, writer in self.request_queue:
            log("apply_log", f"{request.method} {request.args}")
            response = await self.process_request(request)
            response_bytes = pickle.dumps(response)
            if writer is not None:
                writer.write(response_bytes)
                writer.write(SENTINEL)
        self.request_queue = []
        peer_node = self.backups.add_node(peer_node)
        cp = self.get_checkpoint()
        if cp is not None:
            await peer_node.checkpoint(cp, -1)

    @rpc__
    def checkpoint(self, state, checkpoint_num):
        #  print([request.args for request, _ in self.request_queue])
        if checkpoint_num < 0:
            #  Active Recovery
            if not self._ready:
                self.put_checkpoint(state)
                self.request_queue = []
                self._ready = True
                log("checkpoint", f"Checkpoint recovery: {state}")
            else:
                log("discard", f"Checkpoint recovery: {state}")
        else:
            self.put_checkpoint(state)
            self.request_queue = []
            log("checkpoint", f"Checkpoint {checkpoint_num}: {state}")
        self._state_ready = True

    def get_checkpoint(self):
        pass

    def put_checkpoint(self, state):
        pass

    def setAccess(self):
        pass

    async def _serve_remote_call(self, reader, writer):
        """ callback function of asyncio tcp server """
        try:
            #  sock = writer.get_extra_info("socket")
            #  while not reader.at_eof():
                #  Read and parse rpc packet
            request_bytes = await reader.readuntil(SENTINEL)
            request_bytes = request_bytes.strip(SENTINEL)
            request = pickle.loads(request_bytes)
            #  Serve request
            if request.method not in self.__rpcs:
                log("Warning", request.method + " not found")
                return
            if request.method in self.__services:
                if not self._ready:
                    #  log("Not ready", f"{request.method} {request.args} {request.id}")
                    self.request_queue.append((request, writer))
                    return
            response = await self.process_request(request)
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
            return Response(exception=e)

    #  def __get__(self, instance, owner=None):
    #      #  Used as a remote node descriptor
    #      if not instance:
    #          return self
    #      if self._RPCNode__type == ONLINE:
    #          print(self._RPCNode__type)
    #          raise NodeException(f"{self.__class__} is not a remote service")
    #          return
    #      if not isinstance(instance, Client):
    #          log("Fatal", f"Service {self} can't be defined in {instance.__class__.__name__} which is not a Client")
    #          sys.exit(1)
    #      if self._RPCNode__type == LOCAL:
    #          self.__make_service()
    #      self._owner = instance
    #      return self


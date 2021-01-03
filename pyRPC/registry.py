import asyncio

from .rpc_node import RPCNode
from .rpc_client import RPCClient
from .framework_common import *
from .common import *


class Registry(RPCNode):

    def __init__(self, passive=False, port=None):
        super().__init__(port=port)
        self._services = {}
        self._passive = passive

    @rpc__
    async def _register_service(self, service_name, node):
        if service_name in self._services:
            pass
        else:
            self._services[service_name] = ServiceManager(passive=self._passive)
            self.create_task(self._services[service_name].server_online(node))


class ServiceManager(RPCClient):
    def __init__(self, passive=False):
        super().__init__()
        self.active_nodes = set()
        self.backup_nodes = set()
        self.passive = passive
        self.primary = None
        self.mem_lock = asyncio.Lock()

    @property
    def membership(self):
        return self.active_nodes | self.backup_nodes

    def set_passive(self, passive):
        self.passive = passive

    async def server_online(self, node):
        async with self.mem_lock:
            if node is None:
                self.print_membership()
                return
            if node in self.membership:
                return
            new_node = self.server.add_node(node)
            if self.passive:
                if self.primary_server is None:
                    self.primary_server = new_node
                    self.active_nodes.add(new_node)
                    await new_node.state_ready()
                    self.create_task(new_node.set_primary(True))
                    self.create_task(self.client.add_server(new_node))
                else:
                    self.backup_nodes.add(new_node)
                    self.create_task(
                        self.primary_server.add_backup(new_node))
            else:
                self.create_task(self.client.add_server(new_node))
                if not self.active_nodes:
                    await new_node.state_ready()
                    await new_node.set_ready()
                    self.active_nodes.add(new_node)
                else:
                    for i in self.active_nodes:
                        #  self.create_task(i.set_active_false())
                        await i.quiesce()

                    # print(next(iter(self.active_nodes)))
                    for i in self.active_nodes:
                        #  self.create_task(i.add_backup(server_node, True))
                        self.create_task(i.restore_peer(new_node))

                    await asyncio.sleep(1)

                    for i in self.active_nodes:
                        self.create_task(i.set_ready())
                    self.active_nodes.add(new_node)
                        #  self.create_task(i.set_active_clear())

                    #  for i in self.active_nodes:
                    #      self.create_task(i.set_active())

            self.print_membership()

import asyncio
import pickle
from asyncio import transports
from typing import Optional

from . import server
from .framework_common import *
from .common import *

__all__ = ['RPCServerProtocol', 'RPCClientProtocol']


class AbstractProtocol:
    async def write_request(self, request):
        raise NotImplementedError

    async def read_response(self):
        raise NotImplementedError

    async def read_request(self):
        raise NotImplementedError

    async def write_response(self, response):
        raise NotImplementedError

MAGIC_BYTES = 2
REQ_MAGIC = b'\x3f\xcb'
REP_MAGIC = b'\xec\xa8'
LEN_BYTES = 2
HEADER_BYTES = MAGIC_BYTES + LEN_BYTES 
BYTE_ORDER = 'little'

class PickleProtocol(AbstractProtocol):
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    async def write_request(self, request):
        request_bytes = pickle.dumps(request)
        self.writer.write(request_bytes)
        self.writer.write(SENTINEL)
        await self.writer.drain()

    async def read_response(self):
        response_bytes = await self.reader.readuntil(SENTINEL)
        response = pickle.loads(response_bytes.strip(SENTINEL))
        return response

    async def read_request(self):
        try:
            request_bytes = await self.reader.readuntil(SENTINEL)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            print('Connection dropped')
            return None
        request = pickle.loads(request_bytes.strip(SENTINEL))
        return request

    async def write_response(self, response):
        response_bytes = pickle.dumps(response)
        self.writer.write(response_bytes)
        self.writer.write(SENTINEL)
        await self.writer.drain()

class RPCServerProtocol(asyncio.Protocol):
    def __init__(self, server: server.RPCServer):
        self.server = server
        
    def connection_made(self, transport: transports.Transport) -> None:
        self.transport = transport
        self.buffer = bytearray()
        self._eof = False
        self._wait_bytes = HEADER_BYTES
        self._wait_header = True

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if exc:
            print('Client connection lost:', exc)
        self._eof = True

    def data_received(self, data: bytes) -> None:
        assert not self._eof, 'data received after eof received'
        self.buffer.extend(data)
        while len(self.buffer) >= self._wait_bytes:
            if self._wait_header:
                magic, lenbytes = self.buffer[:MAGIC_BYTES], self.buffer[MAGIC_BYTES:HEADER_BYTES]
                del self.buffer[:HEADER_BYTES]
                if bytes(magic) != REQ_MAGIC:
                    print('invalid bytes received')
                    return
                self._wait_bytes = int.from_bytes(lenbytes, BYTE_ORDER)
            else:
                data = self.buffer[:self._wait_bytes]
                del self.buffer[:self._wait_bytes]
                request = pickle.loads(bytes(data))
                assert isinstance(request, Request), 'Illegal object received from client: Not a Request'
                self.server._loop.create_task(self._handle_request(request))
                #  self._handle_request(request)
                self._wait_bytes = HEADER_BYTES
            self._wait_header = not self._wait_header

    def eof_received(self) -> Optional[bool]:
        self._eof = True
        return False    #  Closes transport

    async def _handle_request(self, request: Request):
        response = await self.server._handle_request(request)
        response_bytes = pickle.dumps(response)
        res_len = len(response_bytes)
        self.transport.write(REP_MAGIC)
        self.transport.write(res_len.to_bytes(LEN_BYTES, BYTE_ORDER))
        self.transport.write(response_bytes)


class RPCClientProtocol(asyncio.Protocol):
    def connection_made(self, transport: transports.Transport) -> None:
        self.transport = transport
        self.buffer = bytearray()
        self._eof = False
        self.waiter = None
        self._wait_bytes = 0
        self._wait_header = True

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if exc:
            print('Exception in server connection:', exc)
        self._eof = True

    async def call_one(self, request):
        if self.waiter is not None:
            raise RPCError('Already waiting for one request.')
        request_bytes = pickle.dumps(request)
        datalen = len(request_bytes)
        self.transport.write(REQ_MAGIC)
        self.transport.write(datalen.to_bytes(LEN_BYTES, BYTE_ORDER))
        self.transport.write(request_bytes)
        self.waiter = asyncio.get_event_loop().create_future()
        self._wait_bytes = HEADER_BYTES
        await self.waiter
        data = self.buffer[:self._wait_bytes]
        del self.buffer[:self._wait_bytes]
        response = pickle.loads(bytes(data))
        assert isinstance(response, Response), 'Illegal object received from server: Not a Response'
        self._wait_header = True
        self._wait_bytes = 0
        self.waiter = None
        return response

    def data_received(self, data: bytes) -> None:
        assert not self._eof, 'data received after eof received'
        self.buffer.extend(data)
        while self._wait_bytes and len(self.buffer) >= self._wait_bytes:
            if self._wait_header:
                magic, lenbytes = self.buffer[:MAGIC_BYTES], self.buffer[MAGIC_BYTES:HEADER_BYTES]
                del self.buffer[:HEADER_BYTES]
                if bytes(magic) != REP_MAGIC:
                    print('invalid bytes received')
                    return
                self._wait_bytes = int.from_bytes(lenbytes, BYTE_ORDER)
            else:
                self.waiter.set_result(None)
                break
            self._wait_header = not self._wait_header

    def eof_received(self) -> Optional[bool]:
        self._eof = True
        return False    #  Closes transport

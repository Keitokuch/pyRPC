import asyncio
import pickle
from asyncio import transports
from typing import Optional

from . import server
from .framework_common import *
from .common import *

__all__ = ['RPCServerProtocol', 'RPCClientProtocol']


REQ_MAGIC = b'\xec\xcb'
REP_MAGIC = b'\xec\xa8'
MAGIC_BYTES = 2
SEQ_BYTES = 2
LEN_BYTES = 4
HEADER_LEN = MAGIC_BYTES + SEQ_BYTES + LEN_BYTES 
SEQ_START = MAGIC_BYTES
SEQ_END = SEQ_START + SEQ_BYTES
LEN_START = SEQ_END
LEN_END = LEN_START + LEN_BYTES
BYTE_ORDER = 'little'


class RPCServerProtocol(asyncio.Protocol):
    def __init__(self, server: server.RPCServer):
        self.server = server
        self._curr_seq = None
        
    def connection_made(self, transport: transports.Transport) -> None:
        self.transport = transport
        self.buffer = bytearray()
        self._eof = False
        self._wait_bytes = HEADER_LEN

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if exc:
            print('Client connection lost:', exc)
        self._eof = True

    def data_received(self, data: bytes) -> None:
        assert not self._eof, 'data received after eof received'
        self.buffer.extend(data)
        while len(self.buffer) >= self._wait_bytes:
            if self._curr_seq is None:
                magic, seqbytes, lenbytes = self.buffer[:MAGIC_BYTES], self.buffer[SEQ_START:SEQ_END], self.buffer[LEN_START:LEN_END]
                del self.buffer[:HEADER_LEN]
                if bytes(magic) != REQ_MAGIC:
                    print('invalid header received')
                    return
                self._curr_seq = int.from_bytes(seqbytes, BYTE_ORDER)
                self._wait_bytes = int.from_bytes(lenbytes, BYTE_ORDER)
            else:
                data = self.buffer[:self._wait_bytes]
                del self.buffer[:self._wait_bytes]
                request = pickle.loads(bytes(data))
                assert isinstance(request, Request), 'Illegal object received from client: Not a Request'
                self.server._loop.create_task(self._handle_request(request, self._curr_seq))
                self._curr_seq = None
                self._wait_bytes = HEADER_LEN

    def eof_received(self) -> Optional[bool]:
        self._eof = True
        return False    #  Closes transport

    async def _handle_request(self, request: Request, seq: int):
        response = await self.server._handle_request(request)
        response_bytes = pickle.dumps(response)
        res_len = len(response_bytes)
        self.transport.write(REP_MAGIC)
        self.transport.write(seq.to_bytes(SEQ_BYTES, BYTE_ORDER))
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
        self._seq = 0
        self._curr_seq = None
        self._waiters = {}

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if exc:
            print('Exception in server connection:', exc)
        self._eof = True

    async def call_one(self, request):
        if self.waiter is not None:
            raise RPCError('Already waiting for one request.')
        request_bytes = pickle.dumps(request)
        datalen = len(request_bytes)
        self._seq += 1
        self.transport.write(REQ_MAGIC)
        self.transport.write(self._seq.to_bytes(SEQ_BYTES, BYTE_ORDER))
        self.transport.write(datalen.to_bytes(LEN_BYTES, BYTE_ORDER))
        self.transport.write(request_bytes)
        waiter = asyncio.get_event_loop().create_future()
        self._waiters[self._seq] = waiter
        self._wait_bytes = HEADER_LEN
        data = await waiter
        response = pickle.loads(bytes(data))
        assert isinstance(response, Response), 'Illegal object received from server: Not a Response'
        self._wait_bytes = 0
        self.waiter = None
        return response

    def data_received(self, data: bytes) -> None:
        assert not self._eof, 'data received after eof received'
        self.buffer.extend(data)
        while self._wait_bytes and len(self.buffer) >= self._wait_bytes:
            if self._curr_seq is None:
                magic, seqbytes, lenbytes = self.buffer[:MAGIC_BYTES], self.buffer[SEQ_START:SEQ_END], self.buffer[LEN_START:LEN_END]
                del self.buffer[:HEADER_LEN]
                if bytes(magic) != REP_MAGIC:
                    print('invalid bytes received')
                    return
                self._curr_seq = int.from_bytes(seqbytes, BYTE_ORDER)
                self._wait_bytes = int.from_bytes(lenbytes, BYTE_ORDER)
            else:
                data = self.buffer[:self._wait_bytes]
                del self.buffer[:self._wait_bytes]
                try:
                    waiter = self._waiters[self._curr_seq]
                    waiter.set_result(bytes(data))
                except:
                    print('Got response without request. seq:', self._curr_seq)
                self._wait_bytes = 0
                self._curr_seq = None

    def eof_received(self) -> Optional[bool]:
        self._eof = True
        return False    #  Closes transport

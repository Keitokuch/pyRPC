import asyncio
import pickle
from asyncio import transports
from typing import Optional

from . import server
from .framework_common import *
from .common import *

__all__ = ['RPCServerProtocol', 'RPCClientProtocol']


MAJOR_VER = 0
MINOR_VER = 1

REQ_MAGIC = b'\xec\xcb'
REP_MAGIC = b'\xec\xa8'
RESERVED_A = b'\x00'
RESERVED_B = b'\x00\x00'
MAGIC_BYTES = 2
SEQ_BYTES = 2
LEN_BYTES = 4
RESERVED_BYTES = 3
VERSION_BYTE = 2
HEADER_LEN = MAGIC_BYTES + 1 + SEQ_BYTES + RESERVED_BYTES + LEN_BYTES 
SEQ_START = MAGIC_BYTES + 2
SEQ_END = SEQ_START + SEQ_BYTES
LEN_START = SEQ_END + 2
LEN_END = LEN_START + LEN_BYTES
BYTE_ORDER = 'little'

"""
| MAGIC 0 | MAGIC 1 | VERSION | RESERVED |
|        SEQ        |      RESERVED      |
|       REQUEST/RESPONSE BODY LENGTH     |
"""

def _parse_header(header):
    magic = header[:MAGIC_BYTES]
    version = header[VERSION_BYTE]
    seq = header[SEQ_START:SEQ_END]
    length = header[LEN_START:LEN_END]
    return magic, version, seq, length


def _version_from_byte(v):
    return v >> 4, v & 15


def _version_to_byte(major_v, minor_v):
    return ((major_v << 4) + minor_v).to_bytes(1, BYTE_ORDER)


VERSION = _version_to_byte(MAJOR_VER, MINOR_VER)


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
                magic, verbyte, seqbytes, lenbytes = _parse_header(self.buffer)
                del self.buffer[:HEADER_LEN]
                if bytes(magic) != REQ_MAGIC:
                    continue
                major_v, minor_v = _version_from_byte(verbyte)
                if major_v != MAJOR_VER or minor_v != MINOR_VER:
                    print('got request with unmatching version %d.%d' % (major_v, minor_v))
                    continue
                self._curr_seq = int.from_bytes(seqbytes, BYTE_ORDER)
                self._wait_bytes = int.from_bytes(lenbytes, BYTE_ORDER)
            else:
                data = self.buffer[:self._wait_bytes]
                del self.buffer[:self._wait_bytes]
                request = pickle.loads(bytes(data))
                assert isinstance(request, Request), 'Illegal object received from client: Not a Request'
                self.server.create_task(self._handle_request(request, self._curr_seq))
                self._curr_seq = None
                self._wait_bytes = HEADER_LEN

    def eof_received(self) -> Optional[bool]:
        self._eof = True
        return False    #  Closes transport

    async def _handle_request(self, request: Request, seq: int):
        response = await self.server._handle_request(request)
        response_bytes = pickle.dumps(response)
        res_len = len(response_bytes)
        self.transport.write(REP_MAGIC + VERSION + RESERVED_A)
        self.transport.write(seq.to_bytes(SEQ_BYTES, BYTE_ORDER))
        self.transport.write(RESERVED_B)
        self.transport.write(res_len.to_bytes(LEN_BYTES, BYTE_ORDER))
        self.transport.write(response_bytes)


class RPCClientProtocol(asyncio.Protocol):
    def connection_made(self, transport: transports.Transport) -> None:
        self.transport = transport
        self.buffer = bytearray()
        self._eof = False
        self._wait_bytes = HEADER_LEN
        self._wait_header = True
        self._seq = 0
        self._curr_seq = None
        self._waiters = {}

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if exc:
            print('Exception in server connection:', exc)
        self._eof = True

    async def request_one(self, request: Request) -> Response:
        request_bytes = pickle.dumps(request)
        datalen = len(request_bytes)
        self._seq += 1
        seq = self._seq
        self.transport.write(REQ_MAGIC)
        self.transport.write(VERSION)
        self.transport.write(RESERVED_A)
        self.transport.write(seq.to_bytes(SEQ_BYTES, BYTE_ORDER))
        self.transport.write(RESERVED_B)
        self.transport.write(datalen.to_bytes(LEN_BYTES, BYTE_ORDER))
        self.transport.write(request_bytes)
        waiter = asyncio.get_event_loop().create_future()
        self._waiters[seq] = waiter
        data = await waiter
        response = pickle.loads(bytes(data))
        del self._waiters[seq]
        assert isinstance(response, Response), 'Illegal object received from server: Not a Response'
        return response

    def data_received(self, data: bytes) -> None:
        assert not self._eof, 'data received after eof received'
        self.buffer.extend(data)
        while len(self.buffer) >= self._wait_bytes:
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
                self._wait_bytes = HEADER_LEN
                self._curr_seq = None

    def eof_received(self) -> Optional[bool]:
        self._eof = True
        return False    #  Closes transport

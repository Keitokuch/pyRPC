import asyncio
from pyRPC import RPCClient


async def main():
    cli = RPCClient()
    await cli.connect(":12315")
    await cli.call("minus", 5, 2)
    for _ in range(10):
        await cli.call("sub", 11)
        await cli.call("add", 10)
        await asyncio.sleep(0.5)

asyncio.run(main())

#  cli.sync_call("minus", 1, 2)
#  cli.sync_call("minus", a=1, b=2)

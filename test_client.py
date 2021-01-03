import asyncio
from pyRPC import RPCClient

cli = RPCClient()

cli.sync_call("plus", ":12315", 1, 2)
cli.sync_call("plus", ":12315", a=1, b=2)

async def main():
    await cli.call("minus", ":12315", 5, 2)
    for _ in range(10):
        await cli.call("sub", ":12315", 11)
        await cli.call("add", ":12315", 10)
        await asyncio.sleep(0.5)

asyncio.run(main())

cli.sync_call("minus", ":12315", 1, 2)
cli.sync_call("minus", ":12315", a=1, b=2)

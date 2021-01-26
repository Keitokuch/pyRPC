import asyncio
from pyRPC.client import RPCClient

async def main():
    cli = RPCClient().connect(":12315")
    tasks = []
    for _ in range(10):
        tasks.append(cli.async_call('sub', 1))
    results = await asyncio.gather(*tasks)
    for res in results:
        print(res)
    tasks = []
    NUM_CALLS = 100
    para = 10
    epo = NUM_CALLS // para
    for _ in range(epo):
        tasks = []
        for _ in range(para):
            tasks.append(cli.async_call('sub', 1))
        results = await asyncio.gather(*tasks)


asyncio.run(main())

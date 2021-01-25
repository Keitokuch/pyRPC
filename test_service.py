import asyncio
import pyRPC
import time
from calculator import Calculator

def main():
    print('sync')
    c = Calculator().at(":12315")
    c.add(121)
    c.minus(13, 12)
    for _ in range(10):
        c.sub(11)
        c.add(10)
        time.sleep(0.5)

async def amain():
    print('async')
    c = Calculator().at(":12315")
    await c.sub(11)
    await c.minus(5, 2)
    for _ in range(10):
        await c.sub(11)
        await c.add(10)
        await asyncio.sleep(0.5)
    c = Calculator()
    c.add_node(":12315")
    await c.sub(11)
    await c.minus(5, 2)
    for _ in range(10):
        await c.sub(11)
        await c.add(10)
        await asyncio.sleep(0.5)
    #  c.add_node(Calculator().at("raspi.local:12315"))
    #  await c.minus(5, 2)
    #  for _ in range(10):
    #      await c.sub(11)
    #      await c.add(10)
    #      await asyncio.sleep(0.5)

main()
pyRPC.use_sync_service(False)
asyncio.run(amain())

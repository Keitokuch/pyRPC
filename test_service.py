import asyncio
from calculator import Calculator


async def main():
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
    c.add_node(Calculator().at("raspi.local:12315"))
    await c.minus(5, 2)
    for _ in range(10):
        await c.sub(11)
        await c.add(10)
        await asyncio.sleep(0.5)

asyncio.run(main())

import asyncio
from calculator import Calculator

c = Calculator().at(":12315")

async def main():
    await c.sub(11)
    await c.minus(5, 2)
    for _ in range(10):
        await c.sub(11)
        await c.add(10)
        await asyncio.sleep(0.5)

asyncio.run(main())

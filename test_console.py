import pyRPC
from calculator import Calculator

class Client(pyRPC.Service):

    def __init__(self):
        super().__init__()
        self.calc = Calculator().at(port=12315)

    async def main(self, args):
        cmd = args[0]
        if cmd == "a":
            return await self.calc.add(3)
        if cmd == "s":
            return await self.calc.sub(2)
        if cmd == "p":
            return await self.calc.plus(4, 5)
        if cmd == "m":
            return await self.calc.minus(7, 2)

c = Client()
c.console()
c.run(port=0)

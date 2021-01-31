import asyncio
from pyRPC import rpc, Service


class Calculator(Service):

    def init(self):
        self.mem = 0

    @rpc
    def add(self, a):
        self.mem += a
        return self.mem

    @rpc
    def sub(self, a):
        self.mem -= a
        return self.mem

    @rpc
    def plus(self, a, b):
        return a + b

    @rpc
    def minus(self, a, b):
        return a - b

    @rpc
    def stop(self):
        super().stop()

    async def clean_up(self):
        await asyncio.sleep(0.5)
        print('calc clean up completed')

    def on_rpc_called(self, request):
        #  print('called', request.method)
        pass

    def on_rpc_return(self, response):
        #  print('result', response.method, response.result)
        pass


def main():
    calc = Calculator()
    calc.run(port=12315, debug=True)

if __name__ == "__main__":
    main()

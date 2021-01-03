from pyRPC.common import rpc
from pyRPC import Service


class Calculator(Service):

    def __init__(self):
        super().__init__()
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


def main():
    calc = Calculator()
    calc.run(port=12315)

if __name__ == "__main__":
    main()

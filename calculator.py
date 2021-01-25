from pyRPC.common import rpc, sync
from pyRPC import Service


class Calculator(Service):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mem = 0

    @rpc
    def add(self, a):
        self.mem += a
        return self.mem

    @rpc
    def sub(self, a):
        self.mem -= a
        #  self.stop()
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

    def on_rpc_called(self, request):
        #  print("on called")
        pass

    def on_rpc_return(self, response):
        #  print('on return')
        pass


def main():
    calc = Calculator()
    calc.run(port=12315, debug=True)
    #  calc.run_in_thread(port=12315)
    #  import time
    #  time.sleep(5)
    #  calc.stop()
    #  print('contttt')

if __name__ == "__main__":
    main()

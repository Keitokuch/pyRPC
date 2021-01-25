import time

from pyRPC import RPCClient


def main():
    cli = RPCClient()
    cli.connect(":12315")
    cli.call("minus", 5, 2)
    for _ in range(10):
        cli.call("sub", 11)
        cli.call("add", 10)
        time.sleep(0.5)

main()

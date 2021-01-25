from pyRPC import RPCServer

server = RPCServer(tag="testserver")

state = 0

@server.rpc
def add(n):
    global state
    state += n
    return state

@server.rpc
def sub(n):
    global state
    state -= n
    return state

@server.rpc
def plus(a, b):
    return a + b

@server.rpc
def minus(a, b):
    return a - b

server.run(port=12315, debug=False)

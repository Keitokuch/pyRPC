from calculator import Calculator
import time

ports = [3001, 3002, 3003]

srvs = []
def multi():
    for port in ports:
        srvs.append(Calculator().run_in_thread(port=port))
    time.sleep(2)
    print(srvs)

def one():
    c = Calculator(debug=True)
    c.run_in_thread()

def call():
    g = Calculator()
    for port in ports:
        g.add_node(port=port)
    for n in range(2, 5):
        g.add(n)
        time.sleep(1)

def stop():
    for srv in srvs:
        srv.stop()

multi()
call()
stop()

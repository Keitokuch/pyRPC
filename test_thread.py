from calculator import Calculator
import time

def multi():
    srvs = [Calculator() for _ in range(3)]

    for srv in srvs:
        srv.run_in_thread()
    time.sleep(3)
    print(srvs)

def one():
    c = Calculator(debug=True)
    c.run_in_thread()

multi()

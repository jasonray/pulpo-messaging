import datetime
import os


def log(*values: object, flush: bool = False):
    message = ""
    for arg in values:
        if not arg is None:
            message += ' ' + str(arg)

    pid = os.getpid()
    dt = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")

    TEMPLATE = "[p:{pid}]\t[{dt}]\t{message}"
    output = TEMPLATE.format(pid=pid, dt=dt, message=message)

    flush = False

    print(output, flush=flush)

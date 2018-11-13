import zmq
import traceback
import signal
import uuid
import time
from queue import Empty
from multiprocessing import Process, Value, Queue
from subprocess import Popen, TimeoutExpired
from .messages import *
from .process import get_pstree_data


WORKER_CMD = 'ipengine > /dev/null 2> /dev/null'
UPDATE_CYCLE = 5.0 # repeat every 5 seconds

subproc = None
uid = str(uuid.uuid4())
requests = Queue()
running = Value('b', True)


def start_worker_process(cmd):
    global subproc
    if not subproc:
        subproc = Popen(cmd, shell=True)


def get_process_details():
    data = {
        'uid' : uid,
        'type': 'worker'
    }
    if subproc and subproc.poll() is None:
        data['pid'] = subproc.pid
        stats = get_pstree_data(subproc.pid)
        if stats:
            data.update(stats)
    else:
        retcode = None
        if subproc:
            retcode = subproc.returncode
        data['returncode'] = retcode
        data['status'] = 'dead'
    return data


def graceful_exit():
    if subproc and subproc.poll() is None:
        try:
            subproc.send_signal(signal.SIGINT)
            subproc.wait(4) # wait 4 seconds after interrupt request
        except TimeoutExpired:
            # forcefully kill this stubborn guy
            subproc.send_signal(signal.SIGKILL)
        except:
            try:
                subproc.send_signal(signal.SIGKILL)
            except:
                pass
            traceback.print_exc()


def do_continue():
    time.sleep(1)


def do_restart():
    graceful_exit()
    start_worker_process(WORKER_CMD)


def clear_queue(q):
    while True:
        try:
            q.get_nowait()
        except Empty:
            return


def do_exit():
    running.value = 0
    clear_queue(requests)
    requests.put(None)
    graceful_exit()


handles = {
    REQ_CONTINUE : do_continue,
    REQ_EXIT: do_exit,
    REQ_RESTART: do_restart
}


def apply_requests():
    print('Apply loop entering')
    while True:
        try:
            cmd = requests.get(False, 1.0)
            if cmd is None:
                break
            handles[cmd]()
        except Empty:
            pass
        except KeyboardInterrupt:
            pass

    print('Req loop exiting')


def socket_open(context):
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.RCVTIMEO = 1000
    socket.SNDTIMEO = 1000
    socket.connect("tcp://localhost:5555")
    pollin = zmq.Poller()
    pollin.register(socket, zmq.POLLIN)
    pollout = zmq.Poller()
    pollout.register(socket, zmq.POLLOUT)
    return socket, pollin, pollout


def client_loop():
    try:
        print('Client loop entering')
        start_worker_process(WORKER_CMD)
        context = zmq.Context()
        print("Connecting to the server…")
        socket, pollin, pollout = socket_open(context)

        while running.value:
            try:
                data = get_process_details()
                if pollout.poll(1000):
                    socket.send_json(data)
                    ok = False
                    for trial in range(3):
                        if pollin.poll(1000):
                            req_queue = socket.recv_json()
                            ok = True
                    if not ok:
                        raise RuntimeError('cannot receive back')
                    for x in req_queue:
                        requests.put(x)
                time.sleep(UPDATE_CYCLE)
            except KeyboardInterrupt:
                break
            except:
                # close and reopen socket
                socket.close()
                socket, pollin, pollout = socket_open(context)
                traceback.print_exc()
                time.sleep(UPDATE_CYCLE)

    finally:
        do_exit()
    print('Client loop exiting')


def main():
    req_loop = Process(target=apply_requests)
    comm_loop = Process(target=client_loop)
    try:
        req_loop.start()
        comm_loop.start()
        req_loop.join()
        comm_loop.join()
    except KeyboardInterrupt:
        print('exiting...')
        do_exit()
    finally:
        clear_queue(requests)
        req_loop.join()
        comm_loop.join()
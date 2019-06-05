import zmq
import traceback
import signal
import uuid
import time
from queue import Empty
from multiprocessing import Process, Value, Queue
from subprocess import Popen, TimeoutExpired
from functools import partial
import socket
from .messages import *
from .process import get_pstree_data
import logging

logging.basicConfig()

WORKER_CMD = 'ipengine > /dev/null 2> /dev/null'
UPDATE_CYCLE = 5.0 # repeat every 5 seconds


def get_process_details(ctx):
    data = {
        'uid' : ctx['uid'],
        'type': ctx['type'],
        'host': socket.getfqdn()
    }
    if ctx['pid'].value > 0:
        data['pid'] = ctx['pid'].value
        data['status'] = 'running'
        stats = get_pstree_data(ctx['pid'].value)
        if stats:
            data.update(stats)
    else:
        data['returncode'] = ctx['retcode'].value
        data['status'] = 'dead'
    return data


def graceful_exit(ctx):
    subproc = ctx['subproc']
    logger = ctx['logger']
    if subproc and subproc.poll() is None:
        logger.debug('Trying to kill engine process: {}'.format(subproc.pid))
        try:
            try:
                subproc.send_signal(signal.SIGINT)
                subproc.wait(3) # wait 3 seconds after interrupt request
            except TimeoutExpired:
                subproc.send_signal(signal.SIGTERM)
                subproc.wait(1)
        except:
            try:
                # forcefully kill this stubborn guy
                subproc.send_signal(signal.SIGKILL)
            except:
                pass
                traceback.print_exc()


def do_start(ctx):
    logger = ctx['logger']
    if ctx['subproc'] and ctx['subproc'].poll() is None:
        # already running
        return
    else:
        ctx['subproc'] = Popen(ctx['cmd'], shell=True)
        logger.debug('Started Engine subprocess. PID: {}'.format(ctx['subproc'].pid))
    return False


def do_continue(ctx):
    return False


def do_restart(ctx):
    logger = ctx['logger']
    logger.info('Restart command received')
    graceful_exit(ctx)
    do_start(ctx)
    return False


def clear_queue(q):
    while True:
        try:
            q.get_nowait()
        except Empty:
            return


def do_exit(ctx):
    logger = ctx['logger']
    requests = ctx['requests']
    logger.info('Exit message received. Cleaning up...')
    ctx['running'].value = 0
    clear_queue(requests)
    requests.put(None)
    graceful_exit(ctx)
    logger.info('Clean up complete')
    return True


def control_loop(ctx):

    logger = logging.getLogger('CONTROL')
    logger.setLevel(logging.DEBUG)
    logger.info('Entering Control Loop')

    ctx['logger'] = logger
    ctx['subproc'] = None

    handles = {
        REQ_CONTINUE: do_continue,
        REQ_EXIT: do_exit,
        REQ_RESTART: do_restart,
        REQ_START: do_start
    }

    while True:
        try:
            try:
                req = ctx['requests'].get(timeout=1.0)
                if req is None:
                    break
                if handles[req](ctx): # a tru-ish value means break
                    break
            except Empty:
                pass

            # autorestart
            if ctx['auto_restart'].value and (ctx['subproc'] is None or ctx['subproc'].poll() is not None):
                _rc = ctx['retcode'].value if ctx['retcode'].value != -10000 else 'None'
                logger.debug('Engine is not running [retcode: {}]. Starting...'.format(_rc))
                do_start(ctx)

            # update pid and return code
            _pid = None
            _rc = None
            if ctx['subproc']:
                if ctx['subproc'].poll() is None:
                    _pid = ctx['subproc'].pid
                _rc = ctx['subproc'].returncode

            ctx['pid'].value = -1 if _pid is None else _pid
            ctx['retcode'].value = -10000 if _rc is None else _rc

        except KeyboardInterrupt:
            do_exit(ctx)

    logger.info('Exiting Control Loop')


def socket_open(context):
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.RCVTIMEO = 1000
    socket.SNDTIMEO = 1000
    socket.connect("tcp://localhost:5558")
    pollin = zmq.Poller()
    pollin.register(socket, zmq.POLLIN)
    pollout = zmq.Poller()
    pollout.register(socket, zmq.POLLOUT)
    return socket, pollin, pollout


def communication_loop(ctx):

    print('Entering Communications Loop')
    context = zmq.Context()
    print("Connecting to the server…")
    socket, pollin, pollout = socket_open(context)

    while ctx['running'].value:
        try:
            data = get_process_details(ctx)
            if pollout.poll(1000):
                socket.send_json(data)
                ok = False
                req_queue = []
                for trial in range(3):
                    if pollin.poll(1000):
                        req_queue = socket.recv_json()
                        ok = True
                if not ok:
                    print('ERROR: connection to server timed out')
                    socket.close()
                    socket, pollin, pollout = socket_open(context)
                for x in req_queue:
                    ctx['requests'].put(x)
            time.sleep(UPDATE_CYCLE)
        except KeyboardInterrupt:
            ctx['requests'].put(None)
            break
        except:
            # close and reopen socket
            traceback.print_exc()
        time.sleep(UPDATE_CYCLE)

    print('Exiting Communications Loop')


def main(cmd=WORKER_CMD, ptype='worker'):

    ctx = {
        'uid': str(uuid.uuid4()),
        'cmd': cmd,
        'type': ptype,
        'running': Value('b', True),
        'auto_restart': Value('b', True),
        'requests': Queue(),
        'pid': Value('i', -1),
        'retcode': Value('i', -10000)
    }

    req_loop = Process(target=control_loop, args=(ctx, ))
    comm_loop = Process(target=communication_loop, args=(ctx, ))
    try:
        req_loop.start()
        comm_loop.start()
        req_loop.join()
        comm_loop.join()
    except KeyboardInterrupt:
        print('Keyboard interrupt received')
    finally:
        #clear_queue(requests)
        req_loop.join()
        comm_loop.join()
        clear_queue(ctx['requests'])
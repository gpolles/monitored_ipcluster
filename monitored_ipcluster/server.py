#
#   Hello World server in Python
#   Binds REP socket to tcp://*:5555
#   Expects b"Hello" from client, replies with b"World"
#

import time
import zmq
from multiprocessing import Process, Manager
import traceback
from collections import defaultdict
from functools import partial
import logging
logging.basicConfig()

logger = logging.getLogger('SERVER')
logger.setLevel(logging.INFO)

from .messages import *

workers = {}
scheduler = {}
request_queue = defaultdict(list)


def do_nothing(*args, **kwargs):
    pass


def clear_queue(uid):
    if uid in request_queue:
        del request_queue[uid]


def handle_worker(data):
    uid = data['uid']
    if uid not in workers:
        logger.info('New client connection: {}'.format(uid))
        request_queue[uid] = []
    workers[uid] = data
    workers[uid]['_lastreq'] = time.time()
    return {
        'data': request_queue[uid],
        'on_success': partial(clear_queue, uid)
    }


def handle_scheduler(data):
    global scheduler
    uid = data['uid']
    # replace the scheduler
    scheduler = {
        uid: data
    }
    scheduler[uid]['_lastreq'] = time.time()
    return {
        'data': request_queue[uid],
        'on_success': partial(clear_queue, uid)
    }


def handle_other(data):
    return {
        'data': {
            'status': 'failed',
            'reason': 'Unknown command'
        },
        'on_success': do_nothing
    }


def handle_command(data):
    logger.info('Command received: {}'.format(data['cmd']))

    cmd = data.get('cmd', '')
    if cmd == 'restart':
        return req_restart()
    elif cmd == 'exit':
        return req_exit()
    elif cmd == 'info':
        return req_info()
    else:
        return handle_other(data)


def handle_message(data):

    handles = {
        'worker': handle_worker,
        'scheduler': handle_scheduler,
        'command': handle_command,
        'default': handle_other,
    }

    try:
        f = handles[data['type']]
    except KeyError:
        f = handles['default']

    return f(data)


def cull_inactive(timeout=20):
    to_cull = []
    for uid, data in workers.items():
        now = time.time()
        if now - workers[uid]['_lastreq'] > timeout:
            to_cull.append(uid)
            logger.info('culling inactive worker: {}'.format(uid))
    for uid in to_cull:
        try:
            del workers[uid]
            del request_queue[uid]
        except KeyError:
            pass

    to_cull = []
    for uid, data in scheduler.items():
        now = time.time()
        if now - scheduler[uid]['_lastreq'] > timeout:
            to_cull.append(uid)
            logger.info('culling scheduler: {}'.format(uid))
    for uid in to_cull:
        try:
            del scheduler[uid]
            del request_queue[uid]
        except KeyError:
            pass


def cleanup():
    pass


def req_exit():

    for uid in workers:
        request_queue[uid].append(REQ_EXIT)
    for uid in scheduler:
        request_queue[uid].append(REQ_EXIT)

    return {
        'data': {'status': 'ok'},
        'on_success': do_nothing
    }


def req_restart():
    # restart workers
    for uid in workers:
        request_queue[uid].append(REQ_RESTART)
    return {
        'data': {'status': 'ok'},
        'on_success': do_nothing
    }


def req_reset():
    # restart both scheduler and workers
    for uid in scheduler:
        request_queue[uid].append(REQ_RESTART)
    return req_restart()


def req_info():
    import socket as sk
    host = sk.getfqdn()
    n_workers = len(workers)
    if n_workers > 0:
        ave_cpu = sum([w.get('pcpu', 0) for w in workers.values()]) / n_workers
    else:
        ave_cpu = 0

    scheduler_host = 'N/A'
    for uid in scheduler:
        if 'host' in scheduler[uid]:
            scheduler_host = scheduler[uid]['host']
            
    return {
        'data': {
            'status': 'ok',
            'n_workers': n_workers,
            'ave_cpu': ave_cpu,
            'host': host,
            'shost': scheduler_host,
            'scheduler': scheduler,
        },
        'on_success': do_nothing
    }


def ipyparallel_status_loop(out, client_args=None, interval=5):
    from ipyparallel import Client
    if client_args is None:
        client_args = {}
    timeout = client_args.pop('timeout', 10)

    rcl = None
    while True:
        start = time.time()
        try:
            rcl = Client(**client_args, timeout=timeout)
            n_workers = len(rcl.ids)
            qstat = rcl.queue_status()
            rcl.close()
            n_unassigned = qstat['unassigned']
            iworkers = [k for k in qstat.keys() if k != 'unassigned']
            n_tasks = sum([qstat[k]['tasks'] for k in iworkers])
            n_queued = sum([qstat[k]['queue'] for k in iworkers])
            n_working = sum([ 1 for w in iworkers if qstat[w]['tasks'] + qstat[w]['queue'] > 0 ])
            n_pending = n_queued + n_tasks + n_unassigned
            out['n_workers'] = n_workers
            out['n_pending'] = n_pending
            out['n_working'] = n_working
            out['status'] = 'connected'
        except KeyboardInterrupt:
            return
        except:
            out['status'] = 'error'
            out['reason'] = traceback.format_exc()
        finally:
            if rcl is not None:
                rcl.close()
        elapsed = time.time() - start
        next_iteration_timer = max(0., interval - elapsed)
        time.sleep(next_iteration_timer)


def server_loop(ipclient_args=None):
    logger.info('Starting Server Loop')

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.setsockopt(zmq.LINGER, 0)
    socket.bind("tcp://*:5558")

    ctcpsocket = context.socket(zmq.REP)
    ctcpsocket.setsockopt(zmq.LINGER, 0)
    ctcpsocket.bind("tcp://*:5559")

    cunixsocket = context.socket(zmq.REP)
    cunixsocket.setsockopt(zmq.LINGER, 0)
    cunixsocket.bind("ipc://ipyserver.socket")

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    poller.register(ctcpsocket, zmq.POLLIN)
    poller.register(cunixsocket, zmq.POLLIN)

    manager = Manager()
    ipyparallel_data = manager.dict()
    # start the ipyparallel check loop
    p = Process(
        target=ipyparallel_status_loop,
        kwargs={
            'out': ipyparallel_data,
            'client_args': ipclient_args,
        },
        daemon=True
    )
    p.start()

    while True:
        try:
            #  Wait for next request from either client or controller
            evts = poller.poll(1000)
            if evts:
                sock = evts[0][0]
                data = sock.recv_json()
                logger.debug("Received info: \n{}".format(data))
                try:
                    response = handle_message(data)
                    ok = False
                    if sock.poll(timeout=.2, flags=zmq.POLLOUT):
                        sock.send_json(response['data'])
                        response['on_success']()
                        ok = True
                    if not ok:
                        raise RuntimeError('Cannot send back the message')

                except zmq.ZMQError:
                    logger.debug('Error receiving or sending back message')
                    traceback.print_exc()

                except:
                    traceback.print_exc()

            cull_inactive()

        except KeyboardInterrupt:
            logger.info('Interrupt signal received, stopping..')
            cleanup()
            break

    logger.info('Bye.')
#
#   Hello World server in Python
#   Binds REP socket to tcp://*:5555
#   Expects b"Hello" from client, replies with b"World"
#

import time
import zmq
import json
import traceback
from collections import defaultdict

from .messages import *

workers = {}
scheduler = {}
request_queue = defaultdict(list)


def handle_worker(data):
    uid = data['uid']
    workers[uid] = data
    workers[uid]['_lastreq'] = time.time()
    if uid not in workers:
        print('new connection:', uid)
    return uid


def handle_scheduler(data):
    pass


def handle_other(data):
    raise KeyError()


handles = {
    'worker': handle_worker,
    'scheduler': handle_scheduler,
    'default': handle_other,
}


def handle_message(data):
    uid = data['uid']
    handles[data['type']](data)
    return uid


def cull_inactive(timeout=20):
    to_cull = []
    for uid, data in workers.items():
        now = time.time()
        if now - workers[uid]['_lastreq'] > timeout:
            to_cull.append(uid)
            print('culling inactive worker', uid)
    for uid in to_cull:
        del workers[uid]
        del request_queue[uid]


def cleanup():
    pass


def req_exit():
    for uid in workers:
        request_queue[uid].append(REQ_EXIT)


def req_restart():
    for uid in workers:
        request_queue[uid].append(REQ_RESTART)


def server_loop():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.setsockopt(zmq.LINGER, 0)
    socket.bind("tcp://*:5555")

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    pollout = zmq.Poller()
    pollout.register(socket, zmq.POLLOUT)

    while True:
        try:
            #  Wait for next request from client
            evts = poller.poll(1000)
            if evts:
                data = socket.recv_json()
                print("Received info: \n", data)
                try:
                    uid = handle_message(data)
                    reqq = request_queue[uid]
                    reqq.append(REQ_CONTINUE)
                    ok = False
                    if pollout.poll(.2):
                        socket.send_json(reqq)
                        request_queue[uid] = []
                        ok = True
                    if not ok:
                        raise RuntimeError('cannot send back the message')

                except zmq.ZMQError:
                    print('Error receiving or sending back message')
                    traceback.print_exc()

                except:
                    traceback.print_exc()

            cull_inactive()

        except KeyboardInterrupt:
            cleanup()
            break

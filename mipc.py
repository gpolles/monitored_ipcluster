import zmq
import argparse
import time
import sys

def get_info(s, poller, timeout):
    s.send_json({'type': 'command', 'cmd': 'info'})
    reply = wait_for_response(poller, s, timeout)
    if reply['status'] == 'ok':
        print('Control server running on: ', reply['host'])
        print('Scheduler running on: ', reply['shost'])
        print('Number of active workers: ', reply['n_workers'])
        print('Average cpu usage:', reply['ave_cpu'])


def wait_for_response(poller, socket, timeout=5):
    if poller.poll(timeout * 1000):  # 10s timeout in milliseconds
        msg = socket.recv_json()
    else:
        msg = {'status': 'fail'}
        sys.stderr.write("Timeout processing request\n")
    return msg


if __name__ == "__main__":

    timeout = 2

    parser = argparse.ArgumentParser(description='Control a Monitored Ipyparallel Cluster')
    parser.add_argument('cmd', help='Command (restart|shutdown|reset|info|monitor).')
    parser.add_argument('-a', '--address', type=str, default='localhost:5559',
                        help='address of the cluster (default: localhost:5559)')
    parser.add_argument('-s', '--socket', type=str,
                        help='Use a UNIX socket file descriptor instead of TCP')

    args = parser.parse_args()

    addr = 'tcp://' + args.address
    if args.socket is not None:
        addr = 'ipc://' + args.socket

    ctx = zmq.Context()
    s = ctx.socket(zmq.REQ)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(addr)
    poller = zmq.Poller()
    poller.register(s, zmq.POLLIN)

    if args.cmd == 'restart':
        s.send_json({'type': 'command', 'cmd': 'restart'})
        reply = wait_for_response(poller, s, timeout)
        print(reply['status'])

    if args.cmd == 'shutdown':
        s.send_json({'type': 'command', 'cmd': 'exit'})
        reply = wait_for_response(poller, s, timeout)
        print(reply['status'])

    if args.cmd == 'reset':
        s.send_json({'type': 'command', 'cmd': 'reset'})
        reply = wait_for_response(poller, s, timeout)
        print(reply['status'])

    if args.cmd == 'info':
        get_info(s, poller, timeout)

    if args.cmd == 'monitor':
        try:
            while True:
                get_info(s, poller, timeout)
                print('-----------------------------------')
                time.sleep(1)
        except KeyboardInterrupt:
            pass

    s.close()
    ctx.term()
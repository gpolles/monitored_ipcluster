import zmq
import argparse
import time


def get_info(s):
    s.send_json({'type': 'command', 'cmd': 'info'})
    reply = s.recv_json()
    if reply['status'] == 'ok':
        print('Scheduler running on host: ', reply['host'])
        print('Number of active workers: ', reply['n_workers'])
        print('Average cpu usage:', reply['ave_cpu'])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Control a Monitored Ipyparallel Cluster')
    parser.add_argument('cmd', help='Command (restart|shutdown|reset|info|monitor).')
    parser.add_argument('-a', '--address', type=str, default='localhost:5556',
                        help='address of the cluster (default: localhost:5556)')
    parser.add_argument('-s', '--socket', type=str,
                        help='Use a UNIX socket file descriptor instead of TCP')

    args = parser.parse_args()

    addr = 'tcp://' + args.address
    if args.socket is not None:
        addr = 'ipc://' + args.socket

    ctx = zmq.Context()
    s = ctx.socket(zmq.REQ)
    s.connect(addr)

    if args.cmd == 'restart':
        s.send_json({'type': 'command', 'cmd': 'restart'})
        reply = s.recv_json()
        print(reply['status'])

    if args.cmd == 'shutdown':
        s.send_json({'type': 'command', 'cmd': 'exit'})
        reply = s.recv_json()
        print(reply['status'])

    if args.cmd == 'reset':
        s.send_json({'type': 'command', 'cmd': 'reset'})
        reply = s.recv_json()
        print(reply['status'])

    if args.cmd == 'info':
        get_info(s)

    if args.cmd == 'monitor':
        try:
            while True:
                get_info(s)
                print('-----------------------------------')
                time.sleep(1)
        except KeyboardInterrupt:
            pass

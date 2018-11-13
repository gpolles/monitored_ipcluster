import psutil
import time

_last_net_usage_time = {}
_last_net_usage_vals = {}

from multiprocessing import Pool

_timeout = 1.0
_average_points = 4


def set_params(timeout=1.0, average_points=4):
    global _timeout
    global _average_points
    _timeout = timeout
    _average_points = average_points


def get_process_data(pid):
    net, pcpu, rss, disk_io = 0, 0, 0, 0

    try:

        sp = psutil.Process(pid)

        for i in range(_average_points):
            if sp.is_running():
                net += get_net_usage(pid)
                rss += sp.memory_info().rss
                disk_io += get_disk_io_usage(sp)
                pcpu += sp.cpu_percent(_timeout/_average_points)

    except:
        pass

    return net/_average_points, pcpu/_average_points, rss/_average_points, disk_io/_average_points


def get_net_usage(pid):
    lt = _last_net_usage_time.get(pid, 0)
    lr, lw = _last_net_usage_vals.get(pid, (0, 0))

    r, w = 0, 0

    now = time.time()
    try:
        lines = open('/proc/%d/net/dev' % pid, 'r').readlines()
    except (IOError, FileNotFoundError) as exc:
        raise psutil.NoSuchProcess(pid) from None
    for l in lines[2:]:
        r += int(l.split()[1])
        w += int(l.split()[9])

    rs = (r - lr) / (now - lt)
    ws = (w - lw) / (now - lt)

    _last_net_usage_vals[pid] = (r, w)
    _last_net_usage_time[pid] = now
    return int(rs + ws)


_last_disk_io_usage_time = {}
_last_disk_io_usage_vals = {}


def get_disk_io_usage(p):
    pid = p.pid
    lt = _last_disk_io_usage_time.get(pid, 0)

    lr, lw = _last_disk_io_usage_vals.get(pid, (0, 0))

    r, w = 0, 0

    now = time.time()
    disk_io = p.io_counters()
    r = disk_io.read_bytes
    w = disk_io.write_bytes

    rs = (r - lr) / (now - lt)
    ws = (w - lw) / (now - lt)

    _last_disk_io_usage_vals[pid] = (r, w)
    _last_disk_io_usage_time[pid] = now
    return int(rs + ws)


def get_pstree_data(pid):
    try:
        p = psutil.Process(pid)
        tree = [sp.pid for sp in p.children(True)] + [p]

        net, pcpu, rss, disk_io = 0, 0, 0, 0

        pool = Pool(len(tree))

        for x in pool.map(get_process_data, tree):
            print(x)
            net += x[0]
            pcpu += x[1]
            rss += x[2]
            disk_io += x[3]

        data = {
            'net': net,
            'pcpu': pcpu,
            'rss': rss,
            'disk_io': disk_io
        }

        return data

    except psutil.NoSuchProcess:
        return None

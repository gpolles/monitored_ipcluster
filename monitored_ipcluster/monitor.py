from io import StringIO
import os, os.path, sys
import glob
import time
import numpy as np
from ipyparallel import Client
import ipyparallel
import subprocess
from stat import *
import curses
import multiprocessing
from curses.textpad import Textbox
import queue
import json
HOME = os.environ['HOME']

HIST_SIZE = 3600 # about one hour

status = ''


def human_mem(m):
    if not np.isfinite(m):
        hmem = 'N/A'
    elif m > 1e12:
        hmem = '%4.2f TB' % (float(m)/1024/1024/1024/1024)
    elif m > 1e9:
        hmem = '%4.2f GB' % (float(m)/1024/1024/1024)
    elif m > 1e6:
        hmem = '%4.2f MB' % (float(m)/1024/1024)
    elif m > 1e3:
        hmem = '%4.2f KB' % (float(m)/1024)
    else:
        hmem = '%4d B' % m
    return hmem


def levelbar(x, tot, blen=20):
    try:
        x = float(x)
        tot = float(tot)
        if not np.isfinite(x) or not np.isfinite(tot) or tot == 0:
            raise ValueError()
    except ValueError:
        x = 0
        tot = 1
    n = min( max( round(x / tot * blen), 0 ), blen )
    return '|' * n + ' ' * (blen-n)


def barcolor(x, tot, vals, colors):
    try:
        x = float(x)
        tot = float(tot)
        if not np.isfinite(x) or not np.isfinite(tot) or tot == 0:
            raise ValueError()
    except ValueError:
        x = 0.0
        tot = 1
    v = x/tot
    for ev, col in zip(vals, colors):
        if v < ev:
            return col
    return colors[-1]


class Hist:
    def __init__(self, nmax, *keys):
        self._nmax = nmax
        for k in keys:
            self.__setattr__(k, list())

    def append(self, key, val):
        l = getattr(self, key)
        l.append(val)
        while len(l) > self._nmax:
            l.pop(0)

    def get(self, key):
        return getattr(self, key)


def fix_textbox_special_keys(key):
    if key == 127:
        return 263
    if key == 330:
        return 7
    return key


def start_cluster(stdscr):
    global status
    stdscr.clear()

    curses.curs_set(0)

    curses.init_pair(8, 7, 0)
    curses.init_pair(9, 0, 7)
    curses.init_pair(10, 0, 6)
    NORMAL = curses.color_pair(8)
    HIGHLIGHT = curses.color_pair(9)
    EDIT = curses.color_pair(10)

    my, mx = stdscr.getmaxyx()

    opts = ['nworkers', 'wmem', 'walltime', 'conda', 'smem']
    desc = [
        'Number of workers: ',
        'Memory per worker: ',
        'Walltime: ',
        'Conda environment (leave blank if N/A): ',
        'Scheduler memory: '
    ]
    values = ['400', '2500MB', '100:00:00', 'py3', '4GB']
    edt_wins = [curses.newwin(1, mx - len(x) - 1, i+1, len(x)) for i, x in enumerate(desc)]
    edts = [Textbox(w) for w in edt_wins]
    for i, w in enumerate(edt_wins):
        w.bkgd(' ', NORMAL)
        w.addstr(0, 0, values[i])

    n_opts = len(opts)

    stdscr.addstr(0, 0, 'Start new ipyparallel cluster')

    sel = 0

    while True:
        for i in range(n_opts):
            col = NORMAL
            if i == sel:
                col = HIGHLIGHT
            stdscr.addstr(i+1, 0, desc[i], col)
            #stdscr.addstr(i+1, offset, '%{}s'.format(min_size) % values[i], EDIT)

        stdscr.addstr(n_opts+2, 0, '(ENTER): edit, (ESC): cancel, (CTRL+W) run')

        stdscr.refresh()
        for i, w in enumerate(edt_wins):
            w.refresh()
        k = stdscr.getch()
        stdscr.addstr(n_opts+3, 0, '%10s' % k, HIGHLIGHT)

        if k == 259 and sel > 0:
            sel -= 1
        if k == 258 and sel < n_opts - 1:
            sel += 1
        if k == 10:
            w = edt_wins[sel]
            w.bkgd(' ', EDIT)
            w.refresh()
            curses.setsyx(0,0)
            curses.curs_set(1)
            values[i] = edts[sel].edit(fix_textbox_special_keys)
            curses.curs_set(0)
            w.bkgd(' ', NORMAL)
            w.refresh()
        if k == 27:
            break
        if k == 23:
            status = '%s: Cluster job submitted' % time.asctime()
            subprocess.check_output(['bash', '-c', ' '.join( ['start_ipcluster'] + values )])
            break

def app(stdscr):

    global status
    # suppress stdout and stderr
    old_stderr = sys.stderr
    sys.stderr = StringIO()
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    curses.curs_set(0)

    status = ''

    q0 = multiprocessing.Queue()
    q1 = multiprocessing.Queue()
    client_queue_process = multiprocessing.Process(target=queue_status, args=(q0,), daemon=True)
    client_queue_process.start()

    ps_info_process = multiprocessing.Process(target=monitor_resources, args=(q1,), daemon=True)
    ps_info_process.start()

    maxwnet, maxsnet = 1, 1
    maxwmem, maxsmem = 1, 1
    maxwdisk_io = 1

    whist = Hist(HIST_SIZE, 'mem', 'net', 'disk_io')
    shist = Hist(HIST_SIZE, 'mem', 'net')

    blen = 14
    col1 = 0
    col2 = 40
    label_offs = 11

    try:
        curses.start_color()
        curses.use_default_colors()
        curses.halfdelay(1)
        for i in range(0, 7):
            curses.init_pair(i + 1, i, -1)


        RED = curses.color_pair(2)
        GREEN = curses.color_pair(3)
        YELLOW = curses.color_pair(4)
        GREY = curses.color_pair(9)

        bars_values = [0.49, 0.79, 1.0]
        bars_colors = [GREEN, YELLOW, RED]

        n_workers, n_pending, n_working = np.nan, np.nan, np.nan
        info = {
            'workers': {
                'avecpu' : np.nan,
                'avemem' : np.nan,
                'avenet' : np.nan,
                'avedisk_io' : np.nan
            }
        }

        while True:
            responding = False
            while not q0.empty():
                n_workers, n_pending, n_working = q0.get_nowait()

            if np.isfinite(n_workers):
                responding = True
            else:
                responding = False

            while not q1.empty():
                info = q1.get_nowait()

            #info = monitor_resources()

            stdscr.clear()

            stdscr.addstr(0, 0, 'Scheduler:')
            if responding:
                stdscr.addstr(0, col1 + label_offs, 'ONLINE', GREEN )
            else:
                stdscr.addstr(0, col1 + label_offs, 'OFFLINE', RED )

            sinfo = info.get('scheduler', {'cpu': np.nan, 'mem': np.nan, 'net': np.nan})
            cpu, mem, net = sinfo['cpu'], sinfo['mem'], sinfo['net']

            if np.isfinite(mem):
                shist.append('mem', mem)
            if np.isfinite(net):
                shist.append('net', net)

            maxsmem = 2*np.average(shist.mem)
            maxsnet = 2*np.average(shist.net)

            # with open('debug.txt', 'a') as f:
            #     f.write('%f %f | '% ( mem, maxsmem ))
            #     for x in shist.mem:
            #         f.write('%f ' % x )
            #     f.write('\n')

            stdscr.addstr(2, col1, 'CPU' )

            stdscr.addstr(2, col1 + label_offs, '[')
            stdscr.addstr(
                levelbar(cpu, 100, blen),
                barcolor(cpu, 100, bars_values, bars_colors)
            )
            stdscr.addstr('] {:6.2f}%'.format(cpu) )


            stdscr.addstr(3, col1, 'Memory' )

            stdscr.addstr(3, col1 + label_offs, '[')
            stdscr.addstr(
                levelbar(mem, maxsmem, blen),
                barcolor(mem, maxsmem, bars_values, bars_colors)
            )
            stdscr.addstr('] {:s}'.format(human_mem(mem)) )


            stdscr.addstr(4, col1, 'Network' )

            stdscr.addstr(4, col1 + label_offs, '[')
            stdscr.addstr(
                levelbar(net, maxsnet, blen),
                barcolor(net, maxsnet, bars_values, bars_colors)
            )
            stdscr.addstr('] {:s}'.format(human_mem(net)) )



            winfo = info['workers']
            cpu, mem, net, disk_io = winfo['avecpu'], winfo['avemem'], winfo['avenet'], winfo['avedisk_io']

            if np.isfinite(mem):
                whist.append('mem', mem)
            if np.isfinite(net):
                whist.append('net', net)
            if np.isfinite(disk_io):
                whist.append('disk_io', disk_io)

            maxwmem = 2*np.average(whist.mem)
            maxwnet = 2*np.average(whist.net)
            maxwdisk_io = 2*np.average(whist.disk_io)

            stdscr.addstr(0, col2, 'Workers: ')

            if n_workers > 0:
                stdscr.addstr(str(n_workers), GREEN )
            else:
                stdscr.addstr('No data', RED )


            stdscr.addstr(1, col2, 'Tasks: ')
            stdscr.addstr('%s pending, %s running' % ( n_pending, n_working ) )

            # stdscr.addstr(1, col2 + label_offs, '[')
            # stdscr.addstr(
            #     levelbar(n_working, n_workers, blen),
            #     barcolor(n_working, n_workers, bars_values, bars_colors)
            # )
            # stdscr.addstr('] {:d} / {:d}'.format(n_working, n_workers) )

            stdscr.addstr(2, col2, 'CPU' )

            stdscr.addstr(2, col2 + label_offs, '[')
            stdscr.addstr(
                levelbar(cpu, 100, blen),
                barcolor(cpu, 100, bars_values, bars_colors)
            )
            stdscr.addstr('] {:6.2f}%'.format(cpu) )

            stdscr.addstr(3, col2, 'Memory' )

            stdscr.addstr(3, col2 + label_offs, '[')
            stdscr.addstr(
                levelbar(mem, maxwmem, blen),
                barcolor(mem, maxwmem, bars_values, bars_colors)
            )
            stdscr.addstr('] {:s}'.format(human_mem(mem)) )

            stdscr.addstr(4, col2, 'Network' )

            stdscr.addstr(4, col2 + label_offs, '[')
            stdscr.addstr(
                levelbar(net, maxwnet, blen),
                barcolor(net, maxwnet, bars_values, bars_colors)
            )
            stdscr.addstr('] {:s}'.format(human_mem(net)) )

            stdscr.addstr(5, col2, 'Disk I/O' )
            stdscr.addstr(5, col2 + label_offs, '[')
            stdscr.addstr(
                levelbar(disk_io, maxwdisk_io, blen),
                barcolor(disk_io, maxwdisk_io, bars_values, bars_colors)
            )
            stdscr.addstr('] {:s}'.format(human_mem(disk_io)) )

            stdscr.addstr(6, col1, '(q)uit, (k)ill the cluster, (f)orce shutdown, (s)tart new instance')
            stdscr.addstr(7, col1, status, GREY)



            stdscr.refresh()
            try:
                k = stdscr.getkey()
                if k == 'q':
                    raise KeyboardInterrupt()
                elif k == 'k':
                    pk = multiprocessing.Process(target=kill_cluster)
                    status = '%s: Shutdown requested' % time.asctime()
                    pk.start()
                elif k == 's':
                    curses.cbreak()
                    start_cluster(stdscr)
                    curses.halfdelay(1)

            except curses.error:
                pass


    except KeyboardInterrupt:
        return

    except:
        sys.stderr = old_stderr
        sys.stdout = old_stdout
        raise

    finally:
        sys.stderr = old_stderr
        sys.stdout = old_stdout

if __name__ == '__main__':
    curses.wrapper(app)


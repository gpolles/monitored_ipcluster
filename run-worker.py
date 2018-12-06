import sys
from monitored_ipcluster.client import main

if __name__ == '__main__':
    cmd = 'ipengine "{}" > /dev/null 2> /dev/null'.format('" "'.join(sys.argv[1:]))
    main(cmd=cmd, ptype='worker')

import os, sys, select, time, random, copy
from datetime import datetime

def index_of(x, lis):
    for position, item in enumerate(lis):
        if item == x:
            return position
    return -1

TIME_HEADER = '[%H%M%S.%f] '
def log(x):
    t = datetime.now().strftime(TIME_HEADER) if TIME_HEADER else ""
    print("%s%s" % (t, x,))
    sys.stdout.flush()

DEBUG = True
DEBUG_HEADER = True
def fork_and_do(processes_num, f, idle_work=None):
    rt_strs = [ "" for i in range(processes_num)]
    procs = [[0, 0, 0] for i in range(processes_num)]
    for i in range(processes_num):
        r, w = os.pipe()
        pid = os.fork()
        if pid:
            # parent
            os.close(w)
            r = os.fdopen(r)
        else:
            # child
            os.close(r)
            w = os.fdopen(w, 'w')
            sys.stdout = w
            random.seed()
            f(i)
            w.close()
            sys.exit(0)
        #
        procs[i][0] = r
        procs[i][1] = w
        procs[i][2] = pid

    read_list_org = list(map(lambda p: p[0], procs))
    read_list = copy.copy(read_list_org)
    timeout = 0.001 # 1ms
    while read_list:
        ready = select.select(read_list, [], [], timeout)[0]
        if not ready:
            if idle_work:
                idle_work()
        else:
            for file in ready:
                line = file.readline()
                if not line: # EOF, remove file from input list
                    read_list.remove(file)
                else:
                    idx = index_of(file, read_list_org)
                    rt_strs[idx] += line
                    proc = procs[idx]
                    r, w, pid = proc
                    if DEBUG:
                        if DEBUG_HEADER:
                            sys.stdout.write("[C%02d:%d] " % (idx, pid, ))
                        sys.stdout.write(line)
    return rt_strs

def todo():
    t = (int(random.random() * 1000) / 1000.0) * 10
    time.sleep(t)
    log("child: writing (slept %sms)" % int(t*1000))
    t = (int(random.random() * 1000) / 1000.0) * 10
    time.sleep(t)
    log("child: writing (slept %sms)" % int(t*1000))
    t = (int(random.random() * 1000) / 1000.0) * 10
    time.sleep(t)
    log("child: writing (slept %sms)" % int(t*1000))

if __name__ == '__main__':
    print(fork_and_do(3, todo))

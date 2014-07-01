import subprocess, os, sys, random, time
from amqp.connection import Connection
from q4rmq import QueueManager
from datetime import datetime
from dateutil.relativedelta import relativedelta
import fork_util
from fork_util import fork_and_do, log

fork_util.TIME_HEADER = None
fork_util.DEBUG = False

def timedelta_to_millis(d):
    return round((((d.days * 86400) + d.seconds) * 1000) + (d.microseconds / 1000))

def cmd(c):
    proc = subprocess.Popen(c, shell=True, stdout=subprocess.PIPE)
    stdout_value = proc.communicate()[0]
    rt = stdout_value.decode('utf-8')
    return rt

def setup():
    cmd("sudo rabbitmqctl stop_app")
    cmd("sudo rabbitmqctl reset")
    cmd("sudo rabbitmqctl start_app")

def withsetup(f):
    def rt():
        nonlocal f
        setup()
        f()
    return rt

def onetime(f):
    done_flag = False
    def rt():
        nonlocal f, done_flag
        if not done_flag:
            done_flag = True
            f()
    return rt

TESTS = []
def test(f):
    TESTS.append(f)

def queue():
    c = Connection()
    ch = c.channel()
    return QueueManager(channel=ch)

def check(x, s):
    if not x:
        raise Exception(" --- *** Failed " + s)
    else:
        print(" --- OK " + s)

def dequeue_all(q, tag, f=None):
    ret = []
    def cb(x):
        nonlocal q, ret, f
        if x.get('finish'):
            q.stop_listen()
        else:
            if f: f(x)
            ret.append(x)
    q.listen(tag, cb)
    return ret


def dequeue_item_all(q, f=None):
    ret = []
    def cb(x):
        nonlocal q, ret
        if x['c'].get('finish'):
            q.stop_listen()
        else:
            if f: f(x)
            ret.append(x)
    q.listen_item('tag', cb)
    return ret

@test
@withsetup
def enqueue():
    q = queue()
    with q.open():
        _0c = {'the_data': 'must_be'}
        _0  = q.enqueue('tag', _0c)
        _1  = q.enqueue('tag', {'json': 'serializable_data'})
        _2  = q.enqueue('tag', {'more': 'data'})
        _3c = {'more': 'data2'}
        _3  = q.enqueue('tag', _3c)
        _4  = q.enqueue('tag', {'more': 'data3'})
        q.enqueue('tag', {'finish': True})
        c   = q.count('tag')
        check(c['READY'] == 6, 'enqueue1')
        rt  = dequeue_item_all(q)
        check([_0, _1, _2, _3, _4] == list(map(lambda r: r['i'], rt)), 'enqueue2')

@test
@withsetup
def dequeue():
    q = queue()
    with q.open():
        _0c = {'the_data': 'must_be'}
        _1c = {'json': 'serializable_data'}
        _0  = q.enqueue('tag', _0c)
        _1  = q.enqueue('tag', _1c)
        q.enqueue('tag', {'finish': True})
        rt  = dequeue_item_all(q)
        check(rt[0]['c'] == _0c, 'dequeue1')
        check(rt[1]['c'] == _1c, 'dequeue2')

@test
@withsetup
def dequeue_multi_client():
    #
    CLIENT_NUM = 10
    ENQUEUE_NUM = 10000
    enqueue_time = 0
    dequeue_time = 0
    #
    def dequeue(id):
        q = queue()
        with q.open():
            rt = dequeue_all(q, 'tag', lambda d: log(d['id']))
    #
    @onetime
    def enqueue():
        nonlocal enqueue_time, dequeue_time
        q = queue()
        with q.open():
            enqueue_time = datetime.now()
            for i in range(ENQUEUE_NUM):
                q.enqueue('tag', {'id': i})
            for i in range(CLIENT_NUM):
                q.enqueue('tag', {'finish': True})
            enqueue_time = timedelta_to_millis(datetime.now() - enqueue_time)
            dequeue_time = datetime.now()
    #
    expected = []
    for i in range(ENQUEUE_NUM):
        expected.append(i)
    #
    res = fork_and_do(CLIENT_NUM, dequeue, enqueue)
    dequeue_time = timedelta_to_millis(datetime.now() - dequeue_time)
    acc = []
    for p, i in enumerate(res):
        i = i.rstrip().split('\n')
        print('CLIENT_%02d done %d queues.' % (p, len(i)))
        acc = acc + list(map(int, i))
    #
    print("consumed enqueue:%dms, dequeue:%dms" % (enqueue_time, dequeue_time, ))
    check(sorted(acc) == expected, 'dequeue_multi_client')

@test
@withsetup
def dequeue_multi_client_fail_over_error():
    #
    CLIENT_NUM = 5
    ENQUEUE_NUM = 1000
    enqueue_time = 0
    dequeue_time = 0
    #
    def dequeue(id):
        consume_num = 0
        random.seed()
        def on_consume(d):
            nonlocal consume_num
            if ((id < CLIENT_NUM-1) and
                (((ENQUEUE_NUM / CLIENT_NUM) / (random.random() + 1)) < consume_num)):
                raise Exception('EXPECTED ERROR !!')
            else:
                log(d['id'])
                consume_num += 1
        q = queue()
        with q.open():
            rt = dequeue_all(q, 'tag', on_consume)
    #
    @onetime
    def enqueue():
        nonlocal enqueue_time, dequeue_time
        q = queue()
        with q.open():
            enqueue_time = datetime.now()
            for i in range(ENQUEUE_NUM):
                q.enqueue('tag', {'id': i})
            q.enqueue('tag', {'finish': True}, schedule = datetime.now() + relativedelta(seconds=1))
            enqueue_time = timedelta_to_millis(datetime.now() - enqueue_time)
            dequeue_time = datetime.now()
    #
    expected = []
    for i in range(ENQUEUE_NUM):
        expected.append(i)
    #
    res = fork_and_do(CLIENT_NUM, dequeue, enqueue)
    dequeue_time = timedelta_to_millis(datetime.now() - dequeue_time)
    acc = []
    for p, i in enumerate(res):
        i = i.rstrip().split('\n')
        print('CLIENT_%02d done %d queues.' % (p, len(i)))
        acc = acc + list(map(int, i))
    #
    print("consumed time enqueue:%dms, dequeue:%dms" % (enqueue_time, dequeue_time, ))
    try:
        check(sorted(acc) == expected, 'dequeue_multi_client_fail_over_error')
    except:
        print(sorted(acc))
        print(expected)
        raise

@test
@withsetup
def dequeue_multi_client_fail_over_exit():
    #
    CLIENT_NUM = 5
    ENQUEUE_NUM = 1000
    enqueue_time = 0
    dequeue_time = 0
    #
    def dequeue(id):
        consume_num = 0
        random.seed()
        def on_consume(d):
            nonlocal consume_num
            if ((id < CLIENT_NUM-1) and
                (((ENQUEUE_NUM / CLIENT_NUM) / (random.random() + 1)) < consume_num)):
                sys.exit(0)
            else:
                log(d['id'])
                consume_num += 1
        q = queue()
        with q.open():
            rt = dequeue_all(q, 'tag', on_consume)
    #
    @onetime
    def enqueue():
        nonlocal CLIENT_NUM, ENQUEUE_NUM, enqueue_time, dequeue_time
        q = queue()
        with q.open():
            enqueue_time = datetime.now()
            for i in range(ENQUEUE_NUM):
                q.enqueue('tag', {'id': i})
            for i in range(ENQUEUE_NUM):
                q.enqueue('tag', {'finish': True}, schedule = datetime.now() + relativedelta(seconds=1))
            enqueue_time = timedelta_to_millis(datetime.now() - enqueue_time)
            dequeue_time = datetime.now()
    #
    expected = []
    for i in range(ENQUEUE_NUM):
        expected.append(i)
    #
    res = fork_and_do(CLIENT_NUM, dequeue, enqueue)
    dequeue_time = timedelta_to_millis(datetime.now() - dequeue_time)
    acc = []
    for p, i in enumerate(res):
        i = i.rstrip().split('\n')
        print('CLIENT_%02d done %d queues.' % (p, len(i)))
        acc = acc + list(map(int, i))
    #
    print("consumed time enqueue:%dms, dequeue:%dms" % (enqueue_time, dequeue_time, ))
    check(sorted(acc) == expected, 'dequeue_multi_client_fail_over_exit')

# test for many processes.
@test
@withsetup
def dequeue_multi_client_with_scheduling():
    #
    CLIENT_NUM = 20
    ENQUEUE_NUM = 10000
    TIME_RANGE = 10
    enqueue_time = 0
    dequeue_time = 0
    #
    def dequeue(id):
        q = queue()
        with q.open():
            rt = dequeue_all(q, 'tag', lambda d: log(d['id']))
    #
    @onetime
    def enqueue():
        nonlocal enqueue_time, dequeue_time
        q = queue()
        with q.open():
            enqueue_time = datetime.now()
            for i in range(ENQUEUE_NUM):
                q.enqueue('tag', {'id': i}, schedule = datetime.now() + relativedelta(seconds=int(random.random()*TIME_RANGE)))
            for i in range(CLIENT_NUM):
                q.enqueue('tag', {'finish': True}, schedule = datetime.now() + relativedelta(seconds=TIME_RANGE+1))
            enqueue_time = timedelta_to_millis(datetime.now() - enqueue_time)
            dequeue_time = datetime.now()
    #
    expected = []
    for i in range(ENQUEUE_NUM):
        expected.append(i)
    #
    res = fork_and_do(CLIENT_NUM, dequeue, enqueue)
    dequeue_time = timedelta_to_millis(datetime.now() - dequeue_time)
    acc = []
    for p, i in enumerate(res):
        i = i.rstrip().split('\n')
        print('CLIENT_%02d done %d queues.' % (p, len(i)))
        acc = acc + list(map(int, i))
    #
    print("consumed enqueue:%dms, dequeue:%dms" % (enqueue_time, dequeue_time, ))
    check(sorted(acc) == expected, 'dequeue_multi_client_with_scheduling')

# test for scheduling of very long future.
@test
@withsetup
def scheduling_longspan():
    q = queue()
    with q.open():
        q.enqueue('tag', {'the':'data'}, schedule = datetime.now() + relativedelta(hours = 1))
        q.enqueue('tag', {'the':'data'}, schedule = datetime.now() + relativedelta(days = 1))
        q.enqueue('tag', {'the':'data'}, schedule = datetime.now() + relativedelta(months = 1))
        q.enqueue('tag', {'the':'data'}, schedule = datetime.now() + relativedelta(years = 1))
        q.enqueue('tag', {'the':'data'}, schedule = datetime.now() + relativedelta(years = 100))
        check(True, 'scheduling_1')

# Should be able to dequeue after RabbitMQ restart even if RabbitMQ downed with the queues.
@test
@withsetup
def scheduling_2():
    q = queue()
    data = {'data': int(random.random() * 1000) }
    with q.open():
        q.enqueue('tag', data,  schedule = datetime.now() + relativedelta(seconds = 5))
        q.enqueue('tag', {'finish': True}, schedule = datetime.now() + relativedelta(seconds = 6))
    cmd("sudo rabbitmqctl stop_app")
    time.sleep(2)
    cmd("sudo rabbitmqctl start_app")
    q = queue()
    with q.open():
        rt  = dequeue_all(q, 'tag')
        check(rt[0] == data, 'scheduling_2')

# Should be able to dequeue immediately even if the scheduling time overed during RabbitMQ downing.
@test
@withsetup
def scheduling_3():
    q = queue()
    data = {'data': int(random.random() * 1000) }
    with q.open():
        q.enqueue('tag', data,  schedule = datetime.now() + relativedelta(seconds = 1))
        q.enqueue('tag', {'finish': True}, schedule = datetime.now() + relativedelta(seconds = 4))
    cmd("sudo rabbitmqctl stop_app")
    time.sleep(2.5)
    cmd("sudo rabbitmqctl start_app")
    q = queue()
    with q.open():
        rt  = dequeue_all(q, 'tag')
        check(rt[0] == data, 'scheduling_3')

# Havy load test.
@test
@withsetup
def dequeue_multi_client_with_scheduling_2():
    #
    CLIENT_NUM  = 20
    ENQUEUE_NUM = 2000000
    TIME_RANGE  = 86400
    LOG_STEP    = 1000
    write = sys.stdout.write
    flush = sys.stdout.flush
    #
    def dequeue(id):
        done = 0
        def onqueue(d):
            nonlocal done
            log(d['id'])
            done += 1
            if (done % LOG_STEP) == 0:
                write('--- CLIENT_%02d done %d queues ---\n' % (id, done, ))
                flush()
        q = queue()
        with q.open():
            rt = dequeue_all(q, 'tag', onqueue)
    #
    @onetime
    def enqueue():
        q = queue()
        with q.open():
            for i in range(ENQUEUE_NUM):
                q.enqueue('tag', {'id': i}, schedule = datetime.now() + relativedelta(seconds=int(random.random()*TIME_RANGE)))
                if (i % (LOG_STEP * CLIENT_NUM)) == 0:
                    write('--- %d ENQUEUED ---\n' % i)
                    flush()
            for i in range(CLIENT_NUM):
                q.enqueue('tag', {'finish': True}, schedule = datetime.now() + relativedelta(seconds=TIME_RANGE+1))
    #
    expected = []
    for i in range(ENQUEUE_NUM):
        expected.append(i)
    #
    res = fork_and_do(CLIENT_NUM, dequeue, enqueue)
    acc = []
    for p, i in enumerate(res):
        i = i.rstrip().split('\n')
        print('*** CLIENT_%02d done %d queues ***' % (p, len(i)))
        acc = acc + list(map(int, i))
    #
    check(sorted(acc) == expected, 'dequeue_multi_client_with_scheduling_2')


if __name__ == '__main__':
    for tes in TESTS:
        tes()


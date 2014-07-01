import json, random, sys, subprocess, re, select
from contextlib import contextmanager
from amqp.connection import Connection
from amqp.basic_message import Message
from datetime import datetime
from dateutil.relativedelta import relativedelta

def timedelta_to_secs(d):
    return round((d.days * 86400) + d.seconds + (d.microseconds / 1000000.0))

def timedelta_to_millis(d):
    return round((((d.days * 86400) + d.seconds) * 1000) + (d.microseconds / 1000))

def get_millis_to_the_date(d, f=None):
    f = f if f else datetime.now()
    return timedelta_to_millis(d - f)

def round_by_accuracy(x, accuracy):
    return round(x / float(accuracy)) * accuracy

DEBUG = False
def log(x):
    if DEBUG: print(x)

class QueueConsumer(object):

    def __init__(self, tag, manager, on_recv):
        self.tag = tag
        self.manager = manager
        self.channel = self.manager.channel
        self.on_recv = on_recv
        self.id = None

    def callback(self, msg):
        headers = msg.properties.get('application_headers')
        death = headers.get('x-death') if headers else None
        reason = death[0]['reason'] if death else None
        expired = (reason == 'expired')
        body = self.manager.deserializer(msg.body)
        id = body['i']
        schedule = body.get('s')
        res = None
        if schedule and expired:
            ready_time = datetime.strptime(schedule, self.manager.TIME_FORMAT)
            n = datetime.now()
            if ready_time < n:
                log(" [%s] Done Scheduled missed %sms" % (id, timedelta_to_millis(n - ready_time), ))
                res = body
            else:
                self.manager.enqueue(self.tag, body['c'], ready_time)
                log(" [%s] Re-Published -> %s" % (id, schedule, ))
        elif not schedule and not expired:
            log(" [%s] Done Immediately" % id)
            res = body
        else:
            log(" [%s] Not-Done because of (%s)" % (id, reason))
        #
        self.on_recv(res)
        #
        self.channel.basic_ack(msg.delivery_tag)

    def start(self):
        self.stop()
        self.channel.basic_qos(0, 1, False)
        rtag = self.manager.get_tag(self.tag, 'READY')
        self.id = self.channel.basic_consume(queue=rtag, callback=self.callback)
        log(" Start-Consumer for tag: %s id: %s" % (rtag, self.id, ))

    def stop(self):
        if self.id:
            self.channel.basic_cancel(self.id)
            rtag = self.manager.get_tag(self.tag, 'READY')
            log(" Stop-Consumer for tag: %s id: %s" % (rtag, self.id, ))
            self.id = None

    def wait1(self):
        if self.id:
            self.channel.wait()
            return True
        else:
            return False


class QueueManager(object):

    TTL_MAX     = 86400000 # 604800000 one-week # 2147483647 max
    TIME_FORMAT = '%Y%m%d%H%M%S.%f'
    TAGS = {
        'HEADER'   : 'PQ4R',
        'READY'    : 'R',
        'SCHEDULE' : 'S',
        'EXCHANGE' : 'O',
    }
    SCHEDULING_ACCURACY = 1000 # second

    def setup_tag(self, tag):
        rtag = self.get_tag(tag, 'READY')
        if not self.tags_tbl.get(rtag):
            stag = self.get_tag(tag, 'SCHEDULE')
            otag = self.get_tag(tag, 'EXCHANGE')
            self.channel.queue_declare(queue=rtag, durable=True, auto_delete=False)
            self.channel.exchange_declare(exchange=otag, type='topic', durable=True)
            self.channel.queue_bind(exchange=otag, routing_key=stag+'.*', queue=rtag)
            self.tags_tbl[rtag] = True

    def __init__(self, host=None, userid=None, on_dead=lambda m: m, channel=None):
        self.host         = host
        self.userid       = userid
        self.connection   = None
        self.channel      = channel
        self.serializer   = lambda d: json.dumps(d, separators=(',',':'))
        self.deserializer = lambda d: json.loads(d)
        self.on_dead      = on_dead
        self.listenning   = False
        self.consumer     = None
        # self.invoking_queue_id = None
        self.error_times_to_ignore = 3
        self.tags_tbl     = {}

    @contextmanager
    def open(self, channel=None):
        if channel:
            saved_channel = self.channel
            self.channel = channel
            yield self.channel
            self.channel = saved_channel
        elif self.channel:
            yield self.channel
        else:
            try:
                args = {}
                if self.host:   args['host']   = self.host
                if self.userid: args['userid'] = self.userid
                self.connection = Connection(**args)
                self.channel    = self.connection.channel()
                yield self.channel
            finally:
                self.close()
        return

    def close(self):
        if self.channel:
            self.channel.close()
            self.channel = None
        if self.connection:
            self.connection.close()
            self.connection = None

    def compose_tag(self, tags):
        if isinstance(tags, str):
            tags = [tags]
        for tag in tags:
            if not re.match(r'^[A-Za-z0-9_$\-]+$', tag):
                raise Exception('validation error: tag must be matched /^[A-Za-z0-9_$\\-]+$/')
        return ':'.join(tags)

    def get_tag(self, tags, typ):
        return ('%s.%s.%s' % (self.TAGS['HEADER'], self.TAGS[typ], self.compose_tag(tags)))

    def count(self, tag=None):
        # This function is really slow.
        proc = subprocess.Popen("sudo rabbitmqctl list_queues", shell=True, stdout=subprocess.PIPE)
        stdout_value = proc.communicate()[0]
        rt = stdout_value.decode('utf-8')
        tag_count_pairs = map(lambda p: p.split('\t'), rt.rstrip().split('\n')[1:-1])
        rtag = self.get_tag(tag, 'READY')
        stag = self.get_tag(tag, 'SCHEDULE')
        rt = { 'READY': 0, 'SCHEDULED': 0 }
        for tag, count in tag_count_pairs:
            if tag == rtag:
                rt['READY'] = int(count)
            elif tag[0:len(stag)] == stag:
                rt['SCHEDULED'] += int(count)
            else:
                pass
        return rt

    def gensym(self, l=5):
        src = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
        h = ''
        for i in range(l): h = h + random.choice(src)
        return h + '_' + datetime.now().strftime('%H%M%S%f')

    def enqueue(self, tag, data, schedule = None, error_times=0, channel=None):
        with self.open(channel):
            self.setup_tag(tag)
            msg = Message()
            msg.properties['delivery_mode'] = 2
            body = {'i': self.gensym(), 'c': data, 'e': error_times}
            id = None
            if schedule:
                ttl = round_by_accuracy(get_millis_to_the_date(schedule), self.SCHEDULING_ACCURACY)
                ttl1 = min(ttl, self.TTL_MAX)
                stag = ('%s.%s' % (self.get_tag(tag, 'SCHEDULE'), str(ttl1), ))
                self.channel.queue_declare(
                    queue=stag, durable=True, auto_delete=False,
                    arguments = { 'x-message-ttl' : ttl1, "x-dead-letter-exchange" : self.get_tag(tag, 'EXCHANGE') }
                )
                body['s'] = schedule.strftime(self.TIME_FORMAT)
                msg.body = self.serializer(body)
                id = self.channel.basic_publish(msg, routing_key=stag)
                log(" [%s] Sent scheduled -> ttl: %s(%s) schedule: %s" % (body['i'], ttl1, ttl, body['s'], ))
            else:
                rtag = self.get_tag(tag, 'READY')
                msg.body = self.serializer(body)
                id = self.channel.basic_publish(msg, routing_key=rtag)
                log(" [%s] Sent immediate -> tag: %s" % (body['i'], rtag))
            return body['i']

    def notify_dead(self, msg):
        self.on_dead(msg)
        return

#    @contextmanager
#    def dequeue_item(self, tag, channel=None):
#        with self.open(channel):
#            ctag = self.compose_tag(tag)
#            receiver = QueueReceiver.get(ctag, self)
#            res = receiver.fetchone()
#            if res:
#                if ((0 < self.error_times_to_ignore) and
#                    (self.error_times_to_ignore <= res['e'])):
#                    try:
#                        self.notify_dead(res)
#                    except:
#                        pass
#                    yield None
#                else:
#                    try:
#                        yield res
#                    except:
#                        self.enqueue(tag, res['c'], error_times = res['e'] + 1)
#                        raise
#            else:
#                yield None
#            return
# 
#    @contextmanager
#    def dequeue(self, tag, channel=None):
#        with self.dequeue_item(tag, channel) as res:
#            if res:
#                yield res['c']
#            else:
#                yield res
#            return

    def listen_item(self, tag, on_recv, channel=None):
        #
        def listen_item_iter(res):
            nonlocal on_recv
            if res:
                if ((0 < self.error_times_to_ignore) and
                    (self.error_times_to_ignore <= res['e'])):
                    try:
                        self.notify_dead(res)
                    except:
                        pass
                else:
                    on_recv(res)
        #
        self.stop_listen()
        self.listenning = True
        while True:
            if self.listenning:
                with self.open(channel):
                    self.setup_tag(tag)
                    self.consumer = QueueConsumer(tag, self, listen_item_iter)
                    self.consumer.start()
                    try:
                        while self.consumer.wait1():
                            pass
                    finally:
                        self.consumer.stop()
            else:
                return None

    def stop_listen(self):
        if self.listenning:
            if self.consumer:
                self.consumer.stop()
            self.listenning = False

    def listen(self, tag, on_recv, channel=None):
        wrap = lambda d: on_recv(d['c'] if d != None else None)
        self.listen_item(tag, wrap, channel)

    def dequeue_item_immediate(self, tag, channel=None):
        ctag = self.compose_tag(tag)
        with self.open(channel):
            self.setup_tag(tag)
            msg = self.channel.basic_get(tag)
            if msg:
                headers = msg.properties.get('application_headers')
                death = headers.get('x-death') if headers else None
                reason = death[0]['reason'] if death else None
                expired = (reason == 'expired')
                body = self.deserializer(msg.body)
                id = body['i']
                schedule = body.get('s')
                #
                self.channel.basic_ack(msg.delivery_tag)
                #
                if schedule and expired:
                    ready_time = datetime.strptime(schedule, self.TIME_FORMAT)
                    n = datetime.now()
                    if ready_time < n:
                        log(" [%s] Done Scheduled missed %sms" % (id, timedelta_to_millis(n - ready_time), ))
                        return body
                    else:
                        self.enqueue(self.tag, body['c'], ready_time)
                        log(" [%s] Re-Published -> %s" % (id, schedule, ))
                elif not schedule and not expired:
                    log(" [%s] Done Immediately" % id)
                    return body
                else:
                    log(" [%s] Not-Done because of (%s)" % (id, reason))
            return None

    def dequeue_immediate(self, tag, channel=None):
        res = self.dequeue_item_immediate(tag, channel)
        return res['c'] if res else None

### py-q4rmq: A simple, fast, scalable and SCHEDULABLE message queue using RabbitMQ in Python.

```py-q4rmq``` enhances RabbitMQ to be able to use scheduling queues through Python using the idea of [dead-letter-exchange](https://www.rabbitmq.com/dlx.html).
You can enqueue queues with scheduling time and that'll be consumed on the time you specified.
Of course, As its based on RabbitMQ, its enough fast, scalable and durable.
You can shut down RabbitMQ even if queues exit yet. And they will be consumed after RabbitMQ restarted.
If the scheduled time of them have passed yet then they will be consumed immediately, otherwise will be delayed to the scheduled time.

The idea is reffered from

 - [jamescarr/rabbitmq-scheduled-delivery](https://github.com/jamescarr/rabbitmq-scheduled-delivery)

__License: MIT__

### How to install py-q4rmq

##### From pip or easy_install

    $ pip install py-q4rmq

##### From source

    $ python ./setup.py install

### Tutorial

#### QueueManager
```python
import q4rmq

q = q4rmq.QueueManager(
    host                     = None,              # The host where RubbitMQ running. If None was set use localhost (default None).
    userid                   = None,              # The user to connect to RabbitMQ. If None was set the user running the script will be used (default None).
    on_dead                  = lambda m: m,       # The doing when the queue caused error more than error_times_to_ignore. (default (lambda m: m) # noting to do )
    error_times_to_ignore    = 3)                 # If the queue caused error more than this time will be ignored or set 0 then all the queues will not be ignored. (default 3)
```

#### Basic Manipurations

##### enqueue

 - ***QueueManager.enqueue(tag, data, schedule=None, error_times=0, channel=None)***

```python
q.enqueue('tag', {'the_data': 'must_be'})
q.enqueue('tag', {'json': 'serializable_data'})
q.enqueue('tag', {'more': 'data'})

## each call will open a conncection to the RabbitMQ so it'll be overhead.
## If you want it be more efficient enqueue in q.open() statement.

with q.open():
    for i in range(10):
        q.enqueue('tag', {'more': 'data(%d)' % i})

## The connection will be closed automatically when it exits from the statement.

## You can set another connection_channel too.
q.enqueue('tag', {'the_data': 'must_be'}, channel=another_connection_channel) # enqueue by using another connection.

```

##### dequeue (NYI)
```python
with q.dequeue('tag') as dq:
    print dq
# => {'the_data': 'must_be'}

with q.dequeue('another-tag') as dq:
    print dq
# => None

```

##### dequeue (with metadatas) (NYI)
```python
with q.dequeue_item('tag') as dq:
    print dq
# => (1, 'tag', '{"the_data":"must_be"}', datetime.datetime(...), 0, None)

with q.dequeue_item('another-tag') as dq:
    print dq
# => None

```

##### listen

 - ***QueueManager.listen(tag, on_recv, channel=None)***
 - ***QueueManager.stop_listen()***

```python
def callback(m):
    if m.get('finish'):
        q.stop_listen() # stop listenning.
    else:
        print(m)
q.listen('tag', callback)
print("Listen-Finished!")

# ... waiting for a queue ...

>>> q.enqueue('tag', {'foo', 'bar'})  # another process pushes a queue.

# => {'foo', 'bar'}                   # get queue immediately
# ... waiting for next queue ...

>>> q.enqueue('tag', {'finish', True})  # another process pushes a queue to finish.

# => Listen-Finished!

# And if error occured in the callback or if the program crashed,
# the queue will be restored or will be derivered to another listener.
```

##### listen (with metadatas)

 - ***QueueManager.listen_item(tag, on_recv, channel=None)***

```python
def callback(m):
    if m.get('finish'):
        q.stop_listen() # stop listenning.
    else:
        print(m)
q.listen_item('tag', callback)

# ... waiting for a queue ...

>>> q.enqueue('tag', {'aaa', 'bbb'})  # another process pushes a queue.

# => {'e': 0, 'c': {'aaa': 'bbb'}, 'i': 'XcwO4_175111571400'} # get queue immediately

# ... waiting for next queue ...

```

##### scheduling behavior
```python
def callback(m):
    if m.get('finish'):
        q.stop_listen() # stop listenning.
    else:
        print(m)
q.listen('tag', callback)
print("Listen-Finished!")

# ... waiting for a queue ...

```

Then start another python repl.

```python
with q.open():
    q.enqueue('tag', {'finish': True}, schedule=datetime.now()+relativedelta(hours=1))
    q.enqueue('tag', {'this_queue': 'will_be_fourth'}, schedule=datetime.now()+relativedelta(minutes=1))
    q.enqueue('tag', {'this_queue': 'will_be_third'},  schedule=datetime.now()+relativedelta(seconds=10))
    q.enqueue('tag', {'this_queue': 'will_be_second'}, schedule=datetime.now()+relativedelta(seconds=1))
    q.enqueue('tag', {'this_queue': 'will_be_first'})

```

Then go back to the first repl.

```python
# => {'this_queue': 'will_be_first'}
# => {'this_queue': 'will_be_second'}

# ... wait for 10 seconds ...

# => {'this_queue': 'will_be_third'}

# ... wait for about 1 minute ...

# => {'this_queue': 'will_be_fourth'}

# ... wait for about an hour (3600secs)

# => Listen-Finished!

```
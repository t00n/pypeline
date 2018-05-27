import logging

logger = logging.getLogger(__name__)


def redis_available():
    try:
        import redis

        # check that a redis server is available
        r = redis.Redis()
        r.get('test')
        del r
        return True
    except:
        return False

if redis_available():
    import pickle
    import redis

    class Pipe:
        def __init__(self, name):
            self.message_key = 'pypeline.messages.%s' % name
            self.total_key = 'pypeline.totals.%s' % name
            self.redis = redis.Redis()
            self.in_counter = 0
            self.out_counter = 0

        def send(self, value):
            self.redis.rpush(self.message_key, pickle.dumps(value))
            self.out_counter += 1

        def recv(self):
            value = self.redis.lpop(self.message_key)
            if value is not None:
                value = pickle.loads(value)
                self.in_counter += 1
            return value

        def is_closed(self):
            total = self.redis.get(self.total_key)
            if total is None:
                return False
            total = int(total)
            return total == self.in_counter

        def close(self):
            self.redis.set(self.total_key, self.out_counter)

else:
    from multiprocessing import Pipe as mp_Pipe, Value

    logging.info("Using multiprocessing backend")

    class Pipe:
        def __init__(self, name):
            self.inbound, self.outbound = mp_Pipe(duplex=False)
            self.total = Value('i', -1)
            self.in_counter = 0
            self.out_counter = 0

        def send(self, value):
            self.outbound.send(value)
            self.out_counter += 1

        def recv(self):
            if self.inbound.poll():
                self.in_counter += 1
                return self.inbound.recv()

        def is_closed(self):
            if self.total.value == -1:
                return False
            else:
                return self.total.value == self.in_counter

        def close(self):
            self.total.value = self.out_counter

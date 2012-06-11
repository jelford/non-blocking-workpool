
import multiprocessing
import random
import time
class TaskMaster(multiprocessing.Process):
    def __init__(self, worker_pool, **kwargs):
        super(TaskMaster, self).__init__(**kwargs)
        self.worker_pool = worker_pool
        self.should_continue = True

    def log(self, msg):
        print('taskmaster : {msg}'.format(pid=self.pid, msg=msg))

    def enough(self):
        self.should_continue = False

    def generate_task(self):
        id = random.getrandbits(8)
        def hash_task():
            return ('hash', [random.getrandbits(256)], 'hashtask:{id}'.format(id=id))
        def fib_task():
            return ('fib', [random.randint(1,13)], 'fibtask:{id}'.format(id=id))
        task = random.choice([hash_task, fib_task])()
        self.log('Dispatching: {task}'.format(task=task[2]))
        return task

    def run(self):
        random.seed()
        try:
            while(self.should_continue):
                for i in range(random.randint(0, len(self.worker_pool))):
                    random.choice(self.worker_pool).send(self.generate_task())
                time.sleep(random.randint(0, 5))
        except KeyboardInterrupt:
            pass


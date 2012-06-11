
import multiprocessing
import random
import time

hammer_the_processor = True

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
            return ('fib', [random.randint(24, 30)], 'fibtask:{id}'.format(id=id))
        task = random.choice([hash_task, fib_task])()
        return task

    def run(self):
        random.seed()
        try:
            while(self.should_continue):
                task = self.generate_task()
                task_id = task[2]
                random.choice(self.worker_pool).send(self.generate_task())
                self.log('Dispatched {task_id} at {time}'.format(task_id=task_id, time=time.time()))
                
                if not hammer_the_processor:
                    # Simulate irregular workflow
                    time.sleep(random.randint(0, 250)/1000.0)
        except KeyboardInterrupt:
            pass


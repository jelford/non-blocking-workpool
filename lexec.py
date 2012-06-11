#! /usr/bin/env python
import multiprocessing

from TaskMaster import TaskMaster
from Worker import Worker

try:
    from multiprocessing.connection import wait
except ImportError:
    from select import select
    def wait(object_list, timeout=None):
        return select(object_list, [], [], timeout)[0]

def log(msg):
    print('{pid} : {msg}'.format(pid='root', msg=msg))

def process(msg):
    log(msg)

def event_loop(incoming_channels):
    should_continue = True
    i = 0
    while(True):
        i += 1
        num_processes = len(incoming_channels)
        if num_processes == 0:
            break

        channels_with_data = wait(incoming_channels)
        if channels_with_data is None or len(channels_with_data) == 0:
            continue

        for incoming in channels_with_data:
            try:
                msg = incoming.recv()
            except EOFError:
                incoming_messages.remove(incoming)
            else:
                process(msg)

if __name__ == '__main__':
    import random
    random.seed()

    num_workers = 4
    workers = []
    incoming_data_channels = [] 
    for i in range(num_workers):

        admin_channel = multiprocessing.Pipe()
        results_channel = multiprocessing.Pipe()

        results_send, results_receive = results_channel
        incoming_data_channels.append(results_receive)
        worker = Worker(admin_channel, results_send)
        workers.append(worker)
        worker.start()
  
    task_master = TaskMaster(workers)
    task_master.start()

    try:
        event_loop(incoming_data_channels)
    except KeyboardInterrupt:
        pass

    log('KeyboardInterrupt: Collecting processes and terminating') 
    task_master.enough()
    task_master.join()
    for w in workers:
        w.send(['stop', []])
        w.join()

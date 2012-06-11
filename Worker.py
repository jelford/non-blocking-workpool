
import multiprocessing
from hashlib import sha256
class Worker(multiprocessing.Process):
    unrecognized_command_error = '__UNRECOGNIZED_COMMAND__'
    default_timeout = 0.1

    def __init__(self, incoming_messages, outgoing, **kwargs):
        super(Worker, self).__init__(**kwargs)
        self.incoming_messages_read = incoming_messages[0]
        self.incoming_messages_write = incoming_messages[1]
        self.outgoing_messages_write = outgoing

    def dispatch(self, command, args, ticket_id, return_channel):
        try:
            v = {
                    'stop' : self.stop,
                    'hash' : self.hash,
                    'fib'  : self.fib,
                }[command](*args) 
        except KeyError:
            v = self.unrecognized_command_error

        if return_channel is not None:
            return_channel.send((v, ticket_id))
            self.log('-> {v} [{ticket_id}]'.format(v=v, ticket_id=ticket_id))

    def stop(self):
        self.run = False

    def hash(self, data):
        return sha256(bytes(data)).hexdigest()

    def fib(self, n):
        if n < 2:
            return 1
        else:
            return self.fib(n-1)+self.fib(n-2)

    def run(self):
        self.should_continue = True
        timeout=self.default_timeout
        try:
            while(self.should_continue):
                ready = self.incoming_messages_read.poll(timeout)
                if not ready:
                    # Exponential backoff; don't need to keep polling
                    timeout = min(timeout*2, 2)
                    continue
                
                # Work coming in again; speed up polling.
                timeout = self.default_timeout
                incoming = self.incoming_messages_read.recv()

                self.log('<- {incoming}'.format(incoming=incoming))
                command, args, ticket_id = incoming
                self.dispatch(command, args, ticket_id, self.outgoing_messages_write)
                
        except KeyboardInterrupt:
            pass

        self.outgoing_messages_write.close()
        self.incoming_messages_read.close()

    def send(self, msg):
        self.incoming_messages_write.send(msg)

    def log(self, msg):
        print('{pid} : {msg}'.format(pid=self.pid, msg=msg))


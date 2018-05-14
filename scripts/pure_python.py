#!/usr/bin/env python
from ctypes import c_int
from multiprocessing import Array, Lock as MLock, Process, Queue, RawArray
from math import exp
from random import random
from time import time, sleep
from threading import Lock as TLock, Timer

CONFIG = '../config/config.txt'

# TODO: define worker
class Worker(Process):
    PAD_BYTEC    = b'\x00'

    # request types
    GET_BYTEC    = b'\x00'
    PUT_BYTEC    = b'\x01'
    END_BYTEC    = b'\x02'

    # response types
    EMPTY_BYTEC  = b'\x00'
    ACK_BYTEC    = b'\x06'
    CANCEL_BYTEC = b'\x18'

    def __init__(self, worker_id, table, request_queue, response_queue,
            pending, verbose=False):
        # our assigned worker number
        self.worker_id = worker_id
        # same as above, but as a bytes object
        self.WORKER_ID_BYTEC = worker_id.to_bytes(2, byteorder='big')
        # type: multiprocessing.sharedctypes.RawArray
        self.table = table
        self.table_size = len(table)

        # type: multiprocessing.Queue
        self.request_queue = request_queue
        self.response_queue = response_queue
        # keeps track of which messages are pending 2-phase-commit
        self.pending = pending

        # logging info
        self.verbose = verbose

        # pass options to multiprocessing.Process
        super(Worker, self).__init__(group=None, target=None, name=None)

    def run(self):
        import cProfile
        cProfile.runctx("self.worker_run()", globals(), locals(),
                filename="profiles/server_worker_{}".format(self.worker_id))

    def worker_run(self):
        while True:
            # get the next request as a byte-string, blocking if necessary
            req_bytes, conn_fileno = self.request_queue.get()
            # parse the bytes
            message_id = req_bytes[:2] # we don't need to convert to int
            request_type = req_bytes[2:3]
            key = int.from_bytes(req_bytes[3:5], byteorder='big')
            #if self.verbose:
            #    print("key is", key)
            #    self.show_hex(req_bytes, prefix="message is: ")
            if request_type == self.GET_BYTEC:
                # is there a PUT pending for this key?
                if self.pending[key % self.table_size]:
                    # let client know the location is locked
                    self.respond(conn_fileno, message_id, self.EMPTY_BYTEC,
                            self.EMPTY_BYTEC * 2)
                else:
                    # get the value and respond with it
                    value = self.table[key].to_bytes(2, byteorder='big')
                    self.respond(conn_fileno, message_id, self.ACK_BYTEC,
                            value)
            elif request_type == self.PUT_BYTEC:
                # is there already a pending PUT?
                lock = self.pending.get_lock()
                lock.acquire()
                if self.pending[key]:
                    lock.release()
                    self.respond(conn_fileno, message_id, self.CANCEL_BYTEC,
                            self.EMPTY_BYTEC * 2)
                    value = int.from_bytes(req_bytes[5:7],
                            byteorder='big')
                else:
                    # mark it as pending
                    self.pending[key] = 1
                    lock.release()
                    value = int.from_bytes(req_bytes[5:7],
                            byteorder='big')
                    self.respond(conn_fileno, message_id, self.ACK_BYTEC,
                            self.EMPTY_BYTEC * 2)
            # need commit message before we can PUT value
            elif request_type == self.ACK_BYTEC:
                value = int.from_bytes(req_bytes[5:7], byteorder='big')
                self.table[key] = value
                self.pending[key] = 0
            elif request_type == self.CANCEL_BYTEC:
                self.pending[key] = 0
                value = int.from_bytes(req_bytes[5:7], byteorder='big')
            elif request_type == self.END_BYTEC:
                break
            else:
                print("Got bad request")

    def respond(self, conn_fileno, message_id, response_type, value):
        message = message_id + response_type + value
        if self.verbose:
            self.show_hex(message, prefix="worker responding with: ")
        self.response_queue.put(message)

    def show_hex(self, data, prefix='', suffix='', use_outfile=False):
        """For debugging socket messages"""
        import textwrap
        data = textwrap.wrap(data.hex(), 2)
        print(prefix + ' '.join(data) + suffix)

class Coordinator:
    PAD_BYTEC    = b'\x00'

    # request types
    GET_BYTEC    = b'\x00'
    PUT_BYTEC    = b'\x01'
    END_BYTEC    = b'\x02'

    # response types
    EMPTY_BYTEC  = b'\x00'
    ACK_BYTEC    = b'\x06'
    CANCEL_BYTEC = b'\x18'

    def __init__(self, request_queue, response_queue, options):
        self.options = options

        # messages that are pending *from the coordinator's point of view*
        self.pending = dict()
        self.max_pending = options['backlog']

        # passes messages to/from workers
        self.request_queue = request_queue
        self.response_queue = response_queue

        # get an optimized command generator
        self.generate_command = get_command_generator(
            options['table_size'],
            options['num_test_commands'],
            options['count_every'],
            options['get_frac'],
            options['max_value']
        )

        # show debugging info?
        self.verbose = options['verbose']

    def run(self):
        import cProfile
        cProfile.runctx("self.worker_run()", globals(), locals(),
                filename="profiles/client")

    def worker_run(self):
        # track message handling time
        start_time = time()

        # message sending/receiving loop
        self.pending = dict()
        next_message_id = b'\x00\x00' # start counting at zero
        for command in self.generate_command():
            # assign a message ID
            command = next_message_id + command

            # show debugging info if requested by user
            if self.verbose:
                print("Sending: ", end='')
                self.show_hex(command)

            # assign the command to a worker
            request_queue.put((command, 0))

            # mark the message as pending, and save the args we used
            self.pending[command[:2]] = {
                'args': command[2:],
                'retries': 0,
                'multiplier': None
            }

            # read if there is something to read
            while not self.response_queue.empty():
                response = self.response_queue.get()
                self.handle_response(response)

            # block if there are too many pending requests
            while len(self.pending) > self.max_pending:
                response = self.response_queue.get()
                self.handle_response(response)

            # increment the message counter
            next_message_id = self.incr_message_id(next_message_id)

        # listen until all pending messages have been handled
        while len(self.pending):
            response = self.response_queue.get()
            self.handle_response(response)

        # how long did it take to handle messages?
        end_time = time()
        run_time = end_time - start_time

        print("messaging runtime:", run_time)
        print("throughput:", options['num_test_commands'] / run_time)

        # signal all of the workers to stop
        end_request = b'\x00\x00\x02\x00\x00'
        for worker_id in range(options['server_threads']):
            request_queue.put((end_request, 0))

    @staticmethod
    def incr_message_id(message_id):
        """Returns message, but with its message ID increased by 1"""
        if message_id == b'\xff\xff':
            message_id = b'\x00\x00';
        else:
            message_id = int.from_bytes(message_id, byteorder='big')
            message_id = (message_id + 1).to_bytes(2, byteorder='big')
        return message_id

    def retry(self, message_id):
        message_obj = self.pending[message_id]
        num_retries = message_obj['retries']
        multiplier = message_obj['multiplier']

        if num_retries >= 3:
            num_retries = 0

        # assign a random rate of exponential increase
        if num_retries == 0:
            multiplier = random()
            message_obj['multiplier'] = multiplier

        message_obj['retries'] += 1
        interval = exp(-5 + num_retries * multiplier)
        message = message_id + message_obj['args']
        args = ((message, 0),)
        if self.verbose:
            self.show_hex(message, prefix="Retrying: ")
        timer = Timer(interval, self.request_queue.put, args=args)
        timer.start()

    def handle_response(self, message):
        message_id = message[:2]
        # commit any pending PUTs, because there's only one coordinator
        if message_id in self.pending:
            args = self.pending[message_id]['args']
            request_type = args[0:1]
            if request_type == self.GET_BYTEC:
                if self.verbose:
                    self.show_hex(message, prefix="Received GET response: ")
                response_type = message[2:3]
                if response_type == self.ACK_BYTEC:
                    del self.pending[message_id]
                elif response_type == self.EMPTY_BYTEC:
                    self.retry(message_id)
                else:
                    prefix = "Got bad response message"
                    self.show_hex(message, prefix=prefix)
            elif request_type == self.PUT_BYTEC:
                if self.verbose:
                    self.show_hex(message, prefix="Received PUT response: ")
                response_type = message[2:3]
                if response_type == self.ACK_BYTEC:
                    message = message_id + self.ACK_BYTEC + args[1:]
                    if self.verbose:
                        self.show_hex(message, prefix="Sending back: ")
                    self.request_queue.put((message, 0))
                    del self.pending[message_id]
                elif response_type == self.CANCEL_BYTEC:
                    self.retry(message_id)
                else:
                    prefix = "Got bad response message"
                    self.show_hex(message, prefix=prefix)
            else:
                print(args)
                self.show_hex(message, prefix="Something has gone wrong...")
        else:
            print("Got bad response: ", end='')
            self.show_hex(message)

    def show_hex(self, data, prefix='', suffix='', use_outfile=False):
        """For debugging socket messages"""
        import textwrap
        data = textwrap.wrap(data.hex(), 2)
        print(prefix + ' '.join(data) + suffix)

def get_command_generator(num_keys, num_messages=10000, count_every=0,
        get_frac=0.8, max_value=1000):
    """Returns a generator for random commands"""
    # request types
    PAD_BYTEC   = b'\x00'
    GET_BYTEC   = b'\x00'
    PUT_BYTEC   = b'\x01'
    if count_every > 0:
        def generate_command():
            for i in range(num_messages):
                if not i % count_every:
                    print("i =", i)
                transaction_type = (random() < get_frac) and \
                        GET_BYTEC or PUT_BYTEC
                key = int(random() * num_keys).to_bytes(2, 'big')
                value = int(random() * max_value).to_bytes(2, 'big')
                transaction = transaction_type + key + value
                yield transaction
            print("Last message generated")
    else:
        def generate_command():
            for i in range(num_messages):
                transaction_type = (random() < get_frac) and \
                        GET_BYTEC or PUT_BYTEC
                key = int(random() * num_keys).to_bytes(2, 'big')
                value = int(random() * max_value).to_bytes(2, 'big')
                transaction = transaction_type + key + value
                yield transaction
            print("Last message generated")
    return generate_command

# read and parse configuration options
with open(CONFIG) as fh:
    options = dict()
    for line in fh:
        line = line.strip().split()
        if "." in line[1]:
            line[1] = float(line[1])
        elif line[1].isdecimal():
            line[1] = int(line[1])
        else:
            line[1] = line[1].upper() == "TRUE"
        options[line[0]] = line[1]

if __name__ == '__main__':
    # TODO: finish this
    NUM_WORKERS = options['server_threads']
    # keeps track of messages sent, but not responded to
    pending = Array(c_int, options['table_size'])
    # initialize shared memory
    table = RawArray(c_int, options['table_size'])

    request_queue = Queue()
    response_queue = Queue()

    # run workers as multiprocessing.Process instances
    workers = []
    for worker_id in range(options['server_threads']):
        worker = Worker(worker_id, table, request_queue, response_queue,
                pending, options['verbose'])
        worker.start()
        workers.append(worker)

    # run coordinator in main thread
    coordinator = Coordinator(request_queue, response_queue, options)
    coordinator.run()

import os
import socket
from select import select
from sys import stdout
from time import time, sleep
from multiprocessing import Process
# there is no multiprocessing analogue to threading.Timer
from threading import Lock, Timer
# for determining how much to wait between retries
from math import exp
from random import random

def get_command_generator(num_commands, num_keys, num_messages=10000, count_every=0,
        get_frac=0.8, max_value=1000):
    """Returns a generator for random commands"""
    if count_every > 0:
        def generate_command():
            for i in range(num_messages):
                if not i % count_every:
                    print("i =", i)
                transaction_type = (random() < get_frac) and 'GET' or 'PUT'
                if transaction_type != 'PUT':
                    args = str(int(random() * num_keys))
                else:
                    args = str(int(random() * num_keys)) + ' ' + \
                            str(int(random() * max_value))
                yield transaction_type + ' ' + args
            print("Last message generated")
    else:
        def generate_command():
            for i in range(num_messages):
                transaction_type = (random() < get_frac) and 'GET' or 'PUT'
                if transaction_type != 'PUT':
                    args = str(int(random() * num_keys))
                else:
                    args = str(int(random() * num_keys)) + ' ' + \
                            str(int(random() * max_value))
                yield transaction_type + ' ' + args
            print("Last message generated")
    return generate_command

class Client(Process):
    #
    # Bytecode constants
    #

    # request types
    PAD_BYTEC    = b'\x00' # to keep message length consistent
    GET_BYTEC    = b'\x00'
    PUT_BYTEC    = b'\x01'
    END_BYTEC    = b'\x02'

    # response types
    EMPTY_BYTEC  = b'\x00' # when the response of a GET is null
    ACK_BYTEC    = b'\x06' # OK
    CANCEL_BYTEC = b'\x18' # Abort

    # maximum size of a short
    max_counter = 2 ** 16

    def __init__(self, servers, config):
        """servers should be a list of hostnames"""
        self.config = config

        # general network info
        num_servers = len(servers)
        self.num_nodes = num_servers
        self.num_servers = num_servers
        self.hostname = socket.gethostname()
        self.node_n = servers.index(self.hostname) # our node ID

        # connection settings
        port = int(config['port'])
        self.client_settings = list(zip(servers, (port,) * num_servers))
        self.max_retries = int(config['max_retries'])

        # logging info
        self.verbose = config['verbose'].upper()[0] == 'T'
        outfilename = os.path.join(
                os.getenv("OUTPUT_DIR"),
                self.hostname + "_client.out")
        self.outfile = open(outfilename, 'w')

        # ensure only one thread attempts read/write operation on sockets
        self.sock_lock = Lock()
        # ensure only one thread attempts to modify pending dict
        self.pending_lock = Lock()
        # amount to increase our message ID by
        self.next_message_id = self.node_n
        # amount of pending messages we can have before we just wait
        self.backlog = int(config['backlog'])

        # controls what randomly generated commands look like
        self.generate_command = get_command_generator(
            int(config['num_test_commands']),
            int(config['table_size']),
            int(config['num_test_commands']),
            int(config['count_every']),
            float(config['get_frac']),
            int(config['max_value'])
        )

        # setup thread settings
        super(Client, self).__init__(
            group=None, target=None, name="{} (client)".format(
            self.hostname))

    def run(self):
        # establish a connection with all servers
        connected = []
        client_settings = self.client_settings
        for server in client_settings:
            tries = 0
            self.outfile.write("Attempting connection with " + server[0] +
                    os.linesep)
            self.outfile.flush()
            while tries <= self.max_retries:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                result = s.connect_ex(server)
                if result == 0:
                    connected.append(s)
                    break
                else:
                    self.outfile.write(
                        "Error connecting to {}, retry({}){}".format(
                        server[0], tries, os.linesep))
                    self.outfile.flush()
                    tries += 1
                    sleep(3)
            self.outfile.write("Connection with {} successful{}".format(
                    server[0], os.linesep))
            self.outfile.flush() 
        # start a timer
        self.start_time = time()
        self.outfile.write("Starting message loop at {}\n".format(
                self.start_time))
        self.outfile.flush()
        self.connected = connected

        # process each transaction sequentially (slow)
        num_servers = self.num_servers
        # list of messages sent which have not yet been given a response
        self.pending = dict() 
        for command in self.generate_command():
            if self.verbose:
                self.outfile.write(command + '\n')
                self.outfile.flush()
            # get request type
            request_type = command[:3]
            # get first parameter (key) from the command
            args = list(map(int, command[4:].split()))
            key = args[0]
            # determine which server is responsible for handling this tx
            server_index = key % num_servers
            target_server = connected[server_index]

            # make request to everybody; deal with response later
            for target_server in connected:
                # make the request
                if request_type == 'GET':
                    self.get(target_server, self.next_message_id, key)
                elif request_type == 'PUT':
                    value = args[1]
                    self.put(target_server, self.next_message_id, key,
                            value)

            # make sure messages have unique message ID
            self.next_message_id += self.num_nodes
            # make sure we won't overflow
            self.next_message_id %= self.max_counter

            # check for responses; prevent pending messages from
            # accumulating endlessly; also prevent socket buffer overflow
            self.wait_responses(max_pending=self.backlog)

        # done sending messages, and now we wait for the final responses
        self.wait_responses(max_pending=0)

        # record when we got the last response
        self.end_time = time()
        run_time = self.end_time - self.start_time
        debug_msg = "Messaging run time: {:.2f} s".format(run_time)
        print(debug_msg)
        self.outfile.write(debug_msg + os.linesep)
        debug_msg = "Average throughput: {:.2f} messages/s".format(
                int(self.config['num_test_commands']) / run_time)
        print(debug_msg)
        self.outfile.write(debug_msg + os.linesep)

        # write that we're done to our debug file
        self.outfile.write("Writing {} ENDs{}".format(len(connected),
            os.linesep))
        self.outfile.flush()

        # write that we're done to all servers
        for conn in connected:
            self.request(conn, 0, self.END_BYTEC)

        # we should get sigterm, but if not within 100s, just join thread
        while True:
            sleep(100)

    def wait_responses(self, max_pending=None):
        """Keeps checking for responses until the total number of pending
        messages is no more than max_pending and until there are no more
        unread responses. `None` means don't wait. `0` means keep waiting
        until all requests have received a response.
        """
        if max_pending == None:
            check_responses(block=False)
        else:
            block = True
            responses_read = True
            done = False
            while not done:
                self.pending_lock.acquire()
                if len(self.pending) <= max_pending:
                    block = False
                self.pending_lock.release()
                responses_read = self.check_responses(block=block)
                if not block and not responses_read:
                    done = True

    def check_responses(self, block=False):
        """Check established connections to see if any servers responded
        
        Returns whether any responses were read"""
        responses_read = False

        wlist = tuple()
        xlist = tuple()
        if block:
            # wait for a connection
            ready_list = select(self.connected, wlist, xlist)[0]
        else:
            # poll connections; don't block
            ready_list = select(self.connected, wlist, xlist, 0)[0]
        for conn in ready_list:
            message_id, response_type, data = self.receive_response(conn)
            if message_id == None:
                break
            responses_read = True
            self.pending_lock.acquire()
            # we don't need to handle duplicate responses
            if message_id in self.pending:
                message_log = self.pending[message_id]
                message_type = message_log["args"][1]
                # is the response for a GET?
                if message_type == self.GET_BYTEC:
                    key = int.from_bytes(message_log["args"][2],
                            byteorder='big')
                    if response_type == self.EMPTY_BYTEC:
                        # we got a 'busy' response
                        result = None
                        message_log['responses'] += 1
                        if message_log['responses'] >= self.num_servers:
                            # everyone's busy; retry later
                            message_log['responses'] = 0
                            self.retry(message_id)
                    else:
                        # we got a good response; remove from pending
                        result = int.from_bytes(data, byteorder='big')
                        del self.pending[message_id]
                    debug_msg = "GET {}: {}".format(key, result)
                    if self.verbose:
                        self.outfile.write(debug_msg + '\n')
                # is the response for a PUT?
                elif message_type == self.PUT_BYTEC:
                    key, value = message_log["args"][2:]
                    key = int.from_bytes(key, byteorder='big')
                    value = int.from_bytes(value, byteorder='big')

                    if response_type == self.ACK_BYTEC:
                        # can't commit until everyone's said "OK"
                        message_log['responses'] += 1
                        if self.verbose:
                            self.outfile.write("Got ACK #" +
                                    str(message_log['responses']) + "\n")
                        if message_log['responses'] >= self.num_servers:
                            for target_server in self.connected:
                                if self.verbose:
                                    self.outfile.write("target is " +
                                            repr(target_server) + "\n")
                                self.commit(target_server, message_id)
                            del self.pending[message_id]
                        result = "OK"
                    elif response_type == self.CANCEL_BYTEC:
                        # need to use a different message_id
                        message_log['responses'] = 0
                        if self.verbose:
                            self.outfile.write("got cancel\n")
                            self.outfile.write("message ID is:" + str(
                                message_id) + "\n")
                        new_id = self.next_message_id

                        # incr msg ID counter by an appropriate amount
                        self.next_message_id += self.num_nodes
                        self.next_message_id %= self.max_counter

                        self.pending[new_id] = self.pending[message_id]
                        # make sure args indicates updated message ID
                        message_id, req_type, k, v = \
                                self.pending[new_id]['args']
                        self.pending[new_id]['args'] = (new_id, req_type,
                                k, v)

                        # send everyone else an abort
                        for target_server in self.connected:
                            if not target_server == conn:
                                self.abort(target_server, message_id)

                        if self.verbose:
                            self.outfile.write("deleting pending\n")
                            self.outfile.flush()
                        del self.pending[message_id]
                        if self.verbose:
                            self.outfile.write("pending deleted\n")
                            self.outfile.flush()

                        # retry with new msg ID
                        message_id = new_id
                        if self.verbose:
                            self.outfile.write("retrying with ID " + str(
                                message_id) + "\n")

                        self.retry(message_id)

                        result = "Denied"
                    else:
                        result = "Got bad response message"
                    debug_msg = "PUT {} {}: {}".format(key, value, result)
                    if self.verbose:
                        self.outfile.write(debug_msg + '\n')
            self.pending_lock.release()

            return responses_read

    def show_pending(self, show_ascii=True, show_hex=False,
            show_retries=True, min_pending=None, show_num_pending=False,
            acquire_lock=False):
        """Shows sent messages for which we haven't received a response
        
        Parameters
        ----------
          show_ascii       bool whether to show message as ASCII
          show_hex         bool whether to show message as hexadecimal
          min_pending      int  don't show fewer messages than this number
          show_num_pending int  show how many messages are pending response
        """
        if acquire_lock:
            self.pending_lock.acquire()

        num_pending = len(self.pending)
        if show_num_pending:
            print(num_pending, "messages pending")

        if show_retries:
            values = self.pending.values()
            max_retries = 0
            for value in values:
                if value['retries'] > max_retries:
                    max_retries = value['retries']
            print(max_retries, "retries for at least 1 message")

        if show_ascii or show_hex:
            if min_pending != None and num_pending >= min_pending:
                for message_obj in self.pending.values():
                    args = message_obj["args"]
                    if show_ascii:
                        message = args[0].to_bytes(2, 'big'), *args[1:]
                        key = int.from_bytes(message[2], 'big')
                        if len(message) > 3:
                            value = int.from_bytes(message[3], 'big')
                            debug_msg = "PUT {} {}".format(key, value)
                        else:
                            debug_msg = "GET {}".format(key)
                        print(debug_msg)

                    if show_hex:
                        self.show_hex(b''.join(args[1:]))

        if acquire_lock:
            self.pending_lock.release()

    def receive_response(self, conn):
        """Reads and returns five bytes at a time"""
        # block if necessary
        self.sock_lock.acquire()
        message = conn.recv(5)
        self.sock_lock.release()
        if not message:
            return None, None, None
        if self.verbose:
            self.show_hex(message, prefix="Received: ", use_outfile=True)
        message_id = int.from_bytes(message[:2], byteorder='big')
        response_type = message[2:3]
        data = message[3:5]
        return message_id, response_type, data

    def request(self, conn, message_id, request_type,
            key=PAD_BYTEC*2, value=PAD_BYTEC*2):
        """Sends a bytecode request using the given connection
        
        Parameters
        ----------
            conn            socket  connection for sending
            message_id      int     message chain ID
            request_type    bytes   whether to PUT/GET/END
            key             bytes   key to GET/PUT (leave blank for END)
            value           bytes   value to PUT (PUT only)
        """
        # convert ints to ushort bytes
        message_id = message_id.to_bytes(2, byteorder='big')
        # send concatenated bytes
        self.sock_lock.acquire()
        if self.verbose:
            self.show_hex(message_id + request_type + key + value,
                    prefix="Sending: ", use_outfile=True)
            self.outfile.flush()
        conn.sendall(message_id + request_type + key + value)
        self.sock_lock.release()

    def abort(self, conn, message_id):
        """Tells the server to abort a PUT"""
        req_type = self.CANCEL_BYTEC
        message_obj = self.pending[message_id]
        key, value = message_obj['args'][2:]
        self.request(conn, message_id, req_type, key, value)

    def commit(self, conn, message_id):
        """Tells the server to commit a PUT"""
        req_type = self.ACK_BYTEC
        message_obj = self.pending[message_id]
        key, value = message_obj['args'][2:]
        self.request(conn, message_id, req_type, key, value)

    def get(self, conn, message_id, key):
        """Sends a GET request

        Parameters
        ----------
            conn        socket  connection for sending
            message_id  int     message chain ID
            key         int     key to GET
        """
        req_type = self.GET_BYTEC

        key = key.to_bytes(2, byteorder='big')
        self.request(conn, message_id, req_type, key)

        self.pending_lock.acquire()
        self.pending[message_id] = {
                'args': (message_id, req_type, key),
                'retries': 0,
                'multiplier': None,
                'responses': 0
        } 
        self.pending_lock.release()

    def put(self, conn, message_id, key, value):
        """Sends a PUT request

        Parameters
        ----------
            conn        socket  connection for sending
            message_id  int     message chain ID
            key         int     key to PUT
            value       int     value to PUT
        """
        req_type = self.PUT_BYTEC
        key = key.to_bytes(2, byteorder='big')
        value = value.to_bytes(2, byteorder='big')
        self.request(conn, message_id, req_type, key, value)
        
        self.pending_lock.acquire()
        self.pending[message_id] = {
                'args': (message_id, req_type, key, value),
                'retries': 0,
                'multiplier': None,
                'responses': 0
        } 
        self.pending_lock.release()

    def retry(self, message_id):
        """Waits an exponentially-increasing amount of time, then
        re-attempts a message. The exact rate of exponential increase is
        random."""
        message_obj = self.pending[message_id]
        # how many times have we tried already?
        num_retries = message_obj['retries']
        multiplier = message_obj['multiplier']

        # reset if we already tried 10 times
        if num_retries >= 3:
            num_retries = 0


        # assign a random rate of exponential increase
        if num_retries == 0:
            multiplier = random() * 2
            message_obj['multiplier'] = multiplier

        message_obj['retries'] += 1
        if self.verbose:
            self.outfile.write(str(message_obj['retries']) + " retries ")
            self.outfile.write("for " + str(message_id) + "\n")
        # how long should we wait?
        interval = exp(-5 + num_retries * multiplier)
        for target_server in self.connected:
            args = (target_server,) + message_obj["args"]
            timer = Timer(interval, self.request, args=args)
            timer.start()

    def show_hex(self, data, prefix='', suffix='', use_outfile=False):
        """For debugging socket messages"""
        import textwrap
        data = textwrap.wrap(data.hex(), 2)
        if use_outfile:
            self.outfile.write(prefix + ' '.join(data) + suffix + '\n')
        else:
            print(prefix + ' '.join(data) + suffix)

    def close_all(self):
        """Waits for all nodes to respond (deprecated)"""
        connected = self.connected
        # everyone's connected, so quit
        self.outfile.write("Closing connections{}".format(os.linesep))
        for conn in connected:
            conn.close()
        self.outfile.write("Done ({}){}".format(self.hostname, os.linesep))
        self.outfile.flush()

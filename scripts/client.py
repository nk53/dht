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

def generate_command():
    for i in range(100000):
        if not i % 1000:
            print("i =", i)
        transaction_type = (random() < 0.8) and 'GET' or 'PUT'
        if transaction_type != 'PUT':
            args = str(int(random() * 100))
        else:
            args = str(int(random() * 100)) + ' ' + str(int(random() *
                100))
        yield transaction_type + ' ' + args
    print("Last message generated")

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

    def __init__(self, client_settings, backlog_size, max_retries):
        """client_settings is a list of 2-tuples: (server_hostname, port)"""
        # which node are we?
        self.hostname = socket.gethostname()
        nodes_list = list(zip(*client_settings))
        #self.node_n = list(zip(*client_settings))[0].index(self.hostname)
        self.node_n = nodes_list[0].index(self.hostname)
        # initialize message counter
        self.num_nodes = len(nodes_list[0])
        self.next_message_id = self.node_n

        # general network info
        self.client_settings = client_settings
        self.max_retries = max_retries
        self.num_servers = len(client_settings)

        # open a file for debugging output
        outfilename = os.path.join(
                os.getenv("OUTPUT_DIR"),
                self.hostname + "_client.out")
        self.outfile = open(outfilename, 'w')
        # read transactions from a node-specific file directly into a list
        transaction_filename = os.getenv(
                "TRANSACTION_PREFIX") + str(self.node_n)
        self.transactions = open(transaction_filename)
        with open(transaction_filename) as fh:
            # second item contains number of retries
            self.transactions = [line for line in fh]

        # ensure only one thread attempts read/write operation on sockets
        self.sock_lock = Lock()
        # ensure only one thread attempts to modify pending dict
        self.pending_lock = Lock()

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
                    #self.send_string_message(
                    #    self.next_message_id,
                    #    "{}'s client is alive\n".format(self.hostname),
                    #    s)
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
        #for command in self.transactions:
        for command in generate_command():
            # get request type
            request_type = command[:3]
            # get first parameter (key) from the command
            #key = command[4:command[4:].find(' ') + 4]
            args = list(map(int, command[4:].split()))
            key = args[0]
            # determine which server is responsible for handling this tx
            server_index = key % num_servers
            target_server = connected[server_index]
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

            # have we received any responses?
            self.check_responses()

            # prevent pending messages from accumulating endlessly
            #self.pending_lock.acquire()
            #while len(self.pending) > 1000:
            #    self.pending_lock.release()
            #    self.check_responses()
            #    self.pending_lock.acquire()
            #self.pending_lock.release()

        # done sending messages, and now we wait for the final responses
        self.pending_lock.acquire()
        while self.pending:
            self.pending_lock.release()
            self.check_responses()
            self.pending_lock.acquire()
        self.pending_lock.release()

        # record when we got the last response
        self.end_time = time()
        run_time = self.end_time - self.start_time
        debug_msg = "Total messaging runtime: {} s".format(run_time)
        print(debug_msg)

        self.outfile.write(debug_msg + os.linesep)
        self.outfile.write("Writing {} ENDs{}".format(len(connected),
            os.linesep))
        self.outfile.flush()
        for conn in connected:
            self.request(conn, 0, self.END_BYTEC)

        sleep(100)

    def check_responses(self):
        """Check established connections to see if any servers responded """
        wlist = tuple()
        xlist = tuple()
        # poll connections; don't block
        ready_list = select(self.connected, wlist, xlist, 0)[0]
        for conn in ready_list:
            message_id, response_type, data = self.receive_response(conn)
            if message_id == None:
                break
            self.pending_lock.acquire()
            if message_id in self.pending:
                # TODO: handle response
                message_log = self.pending[message_id]
                message_type = message_log["args"][2]
                # is the response for a GET?
                if message_type == self.GET_BYTEC:
                    key = int.from_bytes(message_log["args"][3],
                            byteorder='big')
                    if response_type == self.EMPTY_BYTEC:
                        result = None
                        self.retry(conn, message_id)
                    else:
                        result = int.from_bytes(data, byteorder='big')
                        del self.pending[message_id]
                    debug_msg = "GET({}): {}".format(key, result)
                    #print(debug_msg)
                    self.outfile.write(debug_msg + '\n')
                # is the response for a PUT?
                elif message_type == self.PUT_BYTEC:
                    key, value = message_log["args"][3:]
                    key = int.from_bytes(key, byteorder='big')
                    value = int.from_bytes(value, byteorder='big')

                    if response_type == self.ACK_BYTEC:
                        # TODO: did every server respond ACK?
                        result = "OK"
                        self.commit(conn, message_id, data)
                        del self.pending[message_id]
                    elif response_type == self.CANCEL_BYTEC:
                        # TODO: put in a list
                        result = "Denied"
                        self.abort(conn, message_id, data)
                        self.retry(conn, message_id)
                    else:
                        result = "Got bad response message"
                    debug_msg = "PUT({}, {}): {}".format(key, value, result)
                    #print(debug_msg)
                    self.outfile.write(debug_msg + '\n')
                # message has been handled, remove it from pending
                #print(len(self.pending), "messages pending")
                #if len(self.pending) < 700:
                #    values = self.pending.values()
                #    max_retries = 0
                #    for value in values:
                #        if value['retries'] > max_retries:
                #            max_retries = value['retries']
                #    print(max_retries, "retries for at least 1 message")
                #    print(len(self.pending), "messages still pending:")
                #if len(self.pending) < 1000:
                #    print(len(self.pending), "messages still pending:")
                #    for message_obj in self.pending.values():
                #        args = message_obj["args"][1:]
                #        message = args[0].to_bytes(2, 'big'), *args[1:]
                #        key = int.from_bytes(message[2], 'big')
                #        if len(message) > 3:
                #            value = int.from_bytes(message[3], 'big')
                #        else:
                #            value = 0
                #        print("PUT({}, {})".format(key, value))
                #        self.show_hex(b''.join(message))
            self.pending_lock.release()

    def receive_response(self, conn):
        """Reads and returns five bytes at a time"""
        # block if necessary
        self.sock_lock.acquire()
        message = conn.recv(5)
        self.sock_lock.release()
        if not message:
            return None, None, None
        #self.show_hex(message, prefix="Received: ")
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
        #self.show_hex(message_id + request_type + key + value,
        #        prefix="Sending: ")
        conn.sendall(message_id + request_type + key + value)
        self.sock_lock.release()

    def abort(self, conn, message_id, worker_id):
        """Tells the server to abort a PUT"""
        req_type = self.CANCEL_BYTEC
        self.request(conn, message_id, req_type, worker_id)

    def commit(self, conn, message_id, worker_id):
        """Tells the server to commit a PUT"""
        req_type = self.ACK_BYTEC
        self.request(conn, message_id, req_type, worker_id)

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
                'args': (conn, message_id, req_type, key),
                'retries': 0
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
                'args': (conn, message_id, req_type, key, value),
                'retries': 0
        } 
        self.pending_lock.release()

    def retry(self, conn, message_id):
        """Waits an exponentially-increasing amount of time, then
        re-attempts a message"""
        message_obj = self.pending[message_id]
        # how many times have we tried already?
        num_retries = message_obj['retries']
        message_obj['retries'] += 1
        # how long should we wait?
        interval = exp(-5 + num_retries) * random()
        timer = Timer(interval, self.request, args=message_obj["args"])
        timer.start()

    def show_hex(self, data, prefix='', suffix=''):
        """For debugging socket messages"""
        import textwrap
        data = textwrap.wrap(data.hex(), 2)
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

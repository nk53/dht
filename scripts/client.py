import os
import socket
from select import select
from sys import stdout
from time import time, sleep
from threading import Thread

class Client(Thread):
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

    def __init__(self, client_settings, backlog_size, max_retries):
        """client_settings is a list of 2-tuples: (server_hostname, port)"""
        # which node are we?
        self.hostname = socket.gethostname()
        nodes_list = list(zip(*client_settings))
        #self.node_n = list(zip(*client_settings))[0].index(self.hostname)
        self.node_n = nodes_list[0].index(self.hostname)
        # initialize message counter
        self.num_nodes = len(nodes_list)
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
            self.transactions = [line for line in fh]

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
        self.next_message_id += self.num_nodes
        # list of messages sent which have not yet been given a response
        self.pending = dict() 
        for command in self.transactions:
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
            # have we received any responses?
            self.check_responses()

        # done sending messages, and now we wait for the final responses
        while self.pending:
            self.check_responses()

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

    def check_responses(self):
        """Check established connections to see if any servers responded """
        wlist = tuple()
        xlist = tuple()
        ready_list = select(self.connected, wlist, xlist)[0]
        for conn in ready_list:
            message_id, response_type, result = \
                    self.receive_response(conn)
            if message_id == None:
                break
            if message_id in self.pending:
                # TODO: handle response
                message_log = self.pending[message_id]
                message_type = message_log["type"]
                # is the response for a GET?
                if message_type == self.GET_BYTEC:
                    if response_type == self.EMPTY_BYTEC:
                        result = None
                    debug_msg = "GET({}): {}".format(
                        message_log["key"], result)
                    print(debug_msg)
                    self.outfile.write(debug_msg + '\n')
                # is the response for a PUT?
                elif message_type == self.PUT_BYTEC:
                    if response_type == self.ACK_BYTEC:
                        result = "OK"
                    else:
                        result = "Denied"
                    debug_msg = "PUT({}, {}): {}".format(
                        message_log["key"], message_log["value"],
                        result)
                    print(debug_msg)
                    self.outfile.write(debug_msg + '\n')
                # message has been handled, remove it from pending
                del self.pending[message_id]


    def receive_response(self, conn):
        """Reads and returns five bytes at a time"""
        message = conn.recv(5)
        if not message:
            return None, None, None
        message_id = int.from_bytes(message[:2], byteorder='big')
        response_type = message[2:3]
        data = int.from_bytes(message[3:5], byteorder='big')
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
        conn.sendall(message_id + request_type + key + value)

    def get(self, conn, message_id, key):
        """Sends a GET request

        Parameters
        ----------
            conn        socket  connection for sending
            message_id  int     mesage chain ID
            key         int     key to GET
        """
        req_type = self.GET_BYTEC
        self.request(
                conn,
                message_id,
                req_type,
                key.to_bytes(2, byteorder='big'))

        self.pending[message_id] = {
                'type': req_type,
                'key': key
        } 
    def put(self, conn, message_id, key, value):
        """Sends a PUT request

        Parameters
        ----------
            conn        socket  connection for sending
            message_id  int     mesage chain ID
            key         int     key to PUT
            value       int     value to PUT
        """
        req_type = self.PUT_BYTEC
        self.request(
                conn,
                message_id,
                req_type,
                key.to_bytes(2, byteorder='big'),
                value.to_bytes(2, byteorder='big'))

        self.pending[message_id] = {
                'type': req_type,
                'key': key,
                'value': value
        } 

    def show_hex(self, data):
        """For debugging socket messages"""
        import textwrap
        data = textwrap.wrap(data.hex(), 2)
        print(' '.join(data))

    def close_all(self):
        """Waits for all nodes to respond (deprecated)"""
        connected = self.connected
        # everyone's connected, so quit
        self.outfile.write("Closing connections{}".format(os.linesep))
        for conn in connected:
            conn.close()
        self.outfile.write("Done ({}){}".format(self.hostname, os.linesep))
        self.outfile.flush()

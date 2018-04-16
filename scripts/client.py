import os
import socket
from select import select
from sys import stdout
from time import sleep
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
        self.outfile.write("Starting message loop\n")
        self.outfile.flush()
        self.connected = connected
        # process each transaction sequentially (slow)
        num_servers = self.num_servers
        self.next_message_id += self.num_nodes
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
            # wait for response if we made a GET request
            if request_type == 'GET':
                self.get(target_server, self.next_message_id, key)
                message_id, response_type, response_data = \
                        self.receive_response(target_server)
                self.outfile.write("GET({}): {}\n".format(key, result))
            elif request_type != 'END':
                value = args[1]
                self.put(target_server, self.next_message_id, key, value)
                message_id, response_type, response_data = \
                        self.receive_response(target_server)
                self.outfile.write("PUT({}, {}): {}{}".format(
                    key, value, result, os.linesep))
            # make sure messages have unique message ID
            self.next_message_id += self.num_nodes
        self.outfile.write("Writing {} ENDs{}".format(len(connected),
            os.linesep))
        self.outfile.flush()
        for conn in connected:
            self.request(conn, 0, self.END_BYTEC)

    def receive_response(self, conn):
        """Reads and returns five bytes at a time"""
        message = conn.recv(5)
        message_id = int.from_bytes(message[:2], byteorder='big')
        response_type = message[2:3]
        data = message[3:5]
        return message_id, response_type, data
        #while len(message) > 0:
        #    message_id = int.from_bytes(message[:2], byteorder='big')
        #    response_type = message[2:3]
        #    data = message[3:5]
        #    yield message_id, response_type, data
        #    message = message[5:]

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
        self.request(
                conn,
                message_id,
                self.GET_BYTEC,
                key.to_bytes(2, byteorder='big'))

    def put(self, conn, message_id, key, value):
        """Sends a PUT request

        Parameters
        ----------
            conn        socket  connection for sending
            message_id  int     mesage chain ID
            key         int     key to PUT
            value       int     value to PUT
        """
        self.request(
                conn,
                message_id,
                self.PUT_BYTEC,
                key.to_bytes(2, byteorder='big'),
                value.to_bytes(2, byteorder='big'))

    def show_hex(self, data):
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

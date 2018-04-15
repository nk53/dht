import os
import socket
from select import select
from sys import stdout
from time import sleep
from threading import Thread

class Client(Thread):
    def __init__(self, client_settings, backlog_size, max_retries):
        # which node are we?
        self.hostname = socket.gethostname()
        self.node_n = list(zip(*client_settings))[0].index(self.hostname)
        # client settings is a list of 2-tuples: (server_hostname, port)
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
                            
                    self.send_string_message(
                        "{}'s client is alive\n".format(self.hostname),
                        s)
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
        self.connected = connected
        # process each transaction sequentially (slow)
        num_servers = self.num_servers
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
            # send the command as a '\n'-terminated string
            target_server.sendall(bytes(command, 'ascii'))
            # wait for response if we made a GET request
            if request_type == 'GET':
                result = self.receive_string_message(target_server)
                self.outfile.write("GET({}): {}\n".format(key, result))
        self.outfile.write("Writing {} ENDs\n".format(len(connected)))
        self.outfile.flush()
        for conn in connected:
            conn.sendall(b'END\n')

    def receive_string_message(self, sender_connection):
        message = sender_connection.recv(1024)
        if message:
            message = message.decode('ascii')
        return message

    def send_string_message(self, message, recipient_socket):
        """Converts ascii message to byte-string, then writes to socket"""
        print(message)
        encoded_m = bytes(message, 'ascii')
        #recipient_socket.sendall(bytes(message, 'ascii'))
        recipient_socket.sendall(encoded_m)

    def close_all(self):
        """Waits for all nodes to respond"""
        connected = self.connected
        # everyone's connected, so quit
        self.outfile.write("Closing connections{}".format(os.linesep))
        for conn in connected:
            conn.close()
        self.outfile.write("Done ({}){}".format(self.hostname, os.linesep))
        self.outfile.flush()

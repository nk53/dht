import os
import socket
from select import select
from sys import stdout
from threading import Thread

class Server(Thread):
    def __init__(self, clients, server_settings, backlog_size, max_retries):
        self.hostname = socket.gethostname()
        print "out dir is:", os.getenv("OUTPUT_DIR")
        self.clients = clients
        self.num_clients = len(clients)
        self.server_settings = server_settings
        self.backlog_size = backlog_size
        self.max_retries = max_retries
        outfilename = os.path.join(
                os.getenv("OUTPUT_DIR"),
                self.hostname + "_server.out")
        self.outfile = open(outfilename, 'w')
        super(Server, self).__init__(
            group=None, target=None, name="%s (server)" % self.hostname)

    def run(self):
        """Setup socket and start listening for connections"""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(self.server_settings)
        s.listen(self.backlog_size)
        # make a socket for each client
        socket_list = []
        self.socket = s
        self.outfile.write("Listening for connections on port (%d)%s" %
                (self.server_settings[1], os.linesep))
        self.outfile.flush()
        self.get_quorum()

    def get_quorum(self):
        """Waits for all nodes to respond"""
        s = self.socket
        connected = []
        clients = self.clients
        while len(connected) != len(clients):
            # block until someone is ready
            conn, addr = s.accept()
            connected.append(conn)

        self.outfile.write("Connected with %s clients%s" % (
            len(connected), os.linesep))
        wlist = tuple()
        xlist = tuple()
        self.outfile.write("Reading messages" + os.linesep)
        self.outfile.flush()
        # keep track of the number of successful PUT operations
        num_puts = 0
        num_done = 0
        while 1:
            ready_list = select(connected, wlist, xlist)[0]
            for conn in ready_list:
                message = conn.recv(1024)
                if not message:
                    break
                # for debugging
                self.outfile.write("Received message from %s%s" % (
                    conn.getpeername(), os.linesep))
                self.outfile.write(message)
                self.outfile.flush()
                lines = message.split('\n')
                self.outfile.write("Got %d lines\n" % len(lines))
                for line in lines:
                    command = line[:3]
                    if command == 'GET':
                        pass # TODO
                    elif command == 'PUT':
                        num_puts += 1
                        self.outfile.write("PUT operations handled: %d%s" % (
                            num_puts, os.linesep))
                        # TODO: finish this
                    elif command == 'END':
                        num_done += 1
                        self.outfile.write("Got END #%d\n" % num_done)
                        self.outfile.flush()
                        if num_done == self.num_clients:
                            # close all connections and shutdown the server
                            for conn in connected:
                                conn.close()
                            self.outfile.write(
                                "Done (%s)%s" % (self.hostname, os.linesep))
                            self.outfile.close()
                            exit()


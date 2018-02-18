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
        wlist = (None,)
        xlist = (None,)
        self.outfile.write("Reading messages" + os.linesep)
        self.outfile.flush()
        while 1:
            ready_list = select(connected, wlist, xlist)
            for conn in ready_list:
                self.outfile.write("Received message from " + conn +
                        os.linesep)
                message = conn.recv(1024)
                self.outfile.write(message)
                self.outfile.flush()
        # everyone's connected, so quit
        for conn in connected:
            conn.close()
        self.outfile.write("Done (%s)%s" % (self.hostname, os.linesep))
        self.outfile.flush()

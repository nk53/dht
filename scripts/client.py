import os
import socket
from select import select
from sys import stdout
from time import sleep
from threading import Thread

class Client(Thread):
    def __init__(self, client_settings, backlog_size, max_retries):
        self.hostname = socket.gethostname()
        self.client_settings = client_settings
        outfilename = os.path.join(
                os.getenv("NODE_OUTPUT"),
                self.hostname + "_server.out")
        print "outfile is:", outfilename
        super(Client, self).__init__(
            group=None, target=None, name="%s (client)" % self.hostname)

    def run(self):
        connected = []
        client_settings = self.client_settings
        for server in client_settings:
            tries = 0
            self.outfile.write("Attempting connection with", server[0])
            self.outfile.flush()
            while tries <= self.max_retries:
                #try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(server)
                connected.append(s)
                s.sendall("%s's client is alive" % self.hostname)
                break
                #except Exception as e:
                #print "Error connecting to %s, retry (%d)" % (
                #        server[0], tries)
                #print "Exception message: %s" % e.message
                #stdout.flush()
                #sleep(3)
                #tries += 1
            self.outfile.write("Connection with %s successful" % server[0])
            self.outfile.flush() 
        self.socket = s
        self.connected = connected
        self.hostname = socket.gethostname()
        self.close_all()

    def close_all(self):
        """Waits for all nodes to respond"""
        connected = self.connected
        # everyone's connected, so quit
        for conn in connected:
            conn.close()
        self.outfile.write("Done (%s)" % self.hostname)
        self.outfile.flush()

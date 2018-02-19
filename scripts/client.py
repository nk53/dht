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
        self.max_retries = max_retries
        outfilename = os.path.join(
                os.getenv("OUTPUT_DIR"),
                self.hostname + "_client.out")
        self.outfile = open(outfilename, 'w')
        super(Client, self).__init__(
            group=None, target=None, name="%s (client)" % self.hostname)

    def run(self):
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
                    s.sendall("%s's client is alive\n" % self.hostname)
                    break
                else:
                    self.outfile.write(
                        "Error connecting to %s, retry(%d)%s" % (
                        server[0], tries, os.linesep))
                    tries += 1
                    sleep(3)
            self.outfile.write("Connection with %s successful%s" %
                    (server[0], os.linesep))
            self.outfile.flush() 
        self.socket = s
        self.connected = connected
        self.hostname = socket.gethostname()
        self.close_all()

    def close_all(self):
        """Waits for all nodes to respond"""
        connected = self.connected
        # everyone's connected, so quit
        self.outfile.write("Closing connections%s" % os.linesep)
        for conn in connected:
            conn.close()
        self.outfile.write("Done (%s)%s" % (self.hostname, os.linesep))
        self.outfile.flush()

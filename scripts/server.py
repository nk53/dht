import os
import socket
from select import select
from sys import stdout
from threading import Thread
from hash_single_thread import Table

class Server(Thread):
    # request types
    GET_BYTEC    = b'\x00'
    PUT_BYTEC    = b'\x01'
    END_BYTEC    = b'\x02'
    
    # response types
    EMPTY_BYTEC  = b'\x00'
    ACK_BYTEC    = b'\x06'
    CANCEL_BYTEC = b'\x18'
    def __init__(self, clients, server_settings, backlog_size, max_retries):
        # setup connection info
        self.hostname = socket.gethostname()
        print("out dir is:", os.getenv("OUTPUT_DIR"))
        self.clients = clients
        self.num_clients = len(clients)
        self.server_settings = server_settings
        self.backlog_size = backlog_size
        self.max_retries = max_retries
        # setup logging info
        outfilename = os.path.join(
                os.getenv("OUTPUT_DIR"),
                self.hostname + "_server.out")
        self.outfile = open(outfilename, 'w')
        # setup table
        self.table = Table(100)
        super(Server, self).__init__(
            group=None, target=None,
            name="{} (server)".format(self.hostname))

    def run(self):
        """Setup socket and start listening for connections"""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(self.server_settings)
        s.listen(self.backlog_size)
        # make a socket for each client
        socket_list = []
        self.socket = s
        self.outfile.write(
                "Listening for connections on port ({}){}".format(
                self.server_settings[1], os.linesep))
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

        self.outfile.write("Connected with {} clients{}".format(
            len(connected), os.linesep))
        self.outfile.flush()
        wlist = tuple()
        xlist = tuple()
        self.outfile.write("Reading messages" + os.linesep)
        self.outfile.flush()
        # keep track of the number of successful PUT operations
        num_puts = 0
        num_done = 0
        done = False
        while not done:
            # block until we get at least one message
            ready_list = select(connected, wlist, xlist)[0]
            for conn in ready_list:
                #message_id, message = self.receive_string_message(conn)
                message = conn.recv(511) # multiple of 7 near 512
                if not message:
                    break
                # for debugging
                #self.outfile.write("Received message from {}{}".format(
                #    conn.getpeername(), os.linesep))
                #lines = message.split('\n')
                for message_id, command, key, value in \
                        self.parse_multiline(message):
                    if command == self.GET_BYTEC:
                        result = self.table.get(key)
                        if result != None:
                            self.respond(conn, message_id, self.ACK_BYTEC,
                                    result)
                        else:
                            self.respond(conn, message_id, self.EMPTY_BYTEC,
                                    0)
                    elif command == self.PUT_BYTEC:
                        result = self.table.put(key, value)
                        self.respond(conn, message_id, self.ACK_BYTEC,
                                result)
                    elif command == self.END_BYTEC:
                        num_done += 1
                        self.outfile.write("Got END #{}{}".format(
                            num_done, os.linesep))
                        self.outfile.flush()
                        if num_done == self.num_clients:
                            # stop server process
                            self.outfile.write(
                                "Done ({}){}".format(
                                self.hostname, os.linesep))
                            self.outfile.close()
                            done = True
                            break

    def respond(self, conn, message_id, response_type, data=b''):
        """Responds with bytecode for yes/no + any additional data
        that was requested by the client (if any)"""
        message_id = message_id.to_bytes(2, byteorder='big')
        if data != b'':
            data = data.to_bytes(2, byteorder='big')
        conn.sendall(message_id + response_type + data)

    def show_hex(self, data):
        """For debugging socket messages"""
        import textwrap
        data = textwrap.wrap(data.hex(), 2)
        print(' '.join(data))

    def parse_multiline(self, data):
        """Returns the next parsed line of multiline input"""
        assert not len(data) % 7, "Got bad message length"
        while len(data):
            message_id   = int.from_bytes(data[:2], byteorder='big')
            request_type = data[2:3]
            key          = int.from_bytes(data[3:5], byteorder='big')
            value        = int.from_bytes(data[5:7], byteorder='big')
            yield message_id, request_type, key, value
            data = data[7:]

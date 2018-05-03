import os
import socket
from select import select
from sys import stdout
from multiprocessing import Array, Lock, Process, Pipe, Queue, RawArray
from hash_single_thread import Table
from ctypes import c_int

class Worker(Process):
    # request types
    GET_BYTEC    = b'\x00'
    PUT_BYTEC    = b'\x01'
    END_BYTEC    = b'\x02'
    
    # response types
    EMPTY_BYTEC  = b'\x00'
    ACK_BYTEC    = b'\x06'
    CANCEL_BYTEC = b'\x18'

    """Uses blocking FIFO queues to read and write to a shared memory
    table"""
    def __init__(self, worker_id, pipe_conn, sock, conns, table,
            request_queue, sock_locks, pending, outfile=None,
            verbose=False):
        # our assigned worker number
        self.worker_id = worker_id
        # same as above, but as a bytes object
        self.WORKER_ID_BYTEC = worker_id.to_bytes(2, byteorder='big')
        # server's socket, shared with us from main thread
        self.socket = sock
        # map a connection's file number to its connection object
        self.connections = dict()
        for conn in conns:
            self.connections[conn.fileno()] = conn
        # for receiving commit/abort messages
        self.pipe = pipe_conn
        # type: multiprocessing.sharedctypes.Array
        self.table = table
        self.table_size = len(table)
        # type: multiprocessing.Queue
        self.request_queue = request_queue
        # ensures two processes don't try to write to the same socket
        self.num_clients = len(sock_locks)
        self.sock_locks = sock_locks
        # keeps track of which messages are pending 2-phase-commit
        self.pending = pending

        # logging info
        self.outfile = outfile
        self.verbose = verbose

        # pass options to multiprocessing.Process
        super(Worker, self).__init__(group=None, target=None, name=None)

    def run(self):
        while True:
            # get the next request as a byte-string, blocking if necessary
            req_bytes, conn_fileno = self.request_queue.get()
            # get connection object from file number
            conn = self.connections[conn_fileno]
            # parse the bytes
            message_id = req_bytes[:2] # we don't need to convert to int
            request_type = req_bytes[2:3]
            key = int.from_bytes(req_bytes[3:5], byteorder='big')
            if request_type == self.GET_BYTEC:
                # is there a PUT pending for this key?
                if self.pending[key % self.table_size]:
                    # let client know the location is locked
                    self.respond(conn, message_id, self.EMPTY_BYTEC,
                            self.EMPTY_BYTEC * 2)
                else:
                    # get the value and respond with it
                    value = self.table[key].to_bytes(2, byteorder='big')
                    self.respond(conn, message_id, self.ACK_BYTEC, value)
            elif request_type == self.PUT_BYTEC:
                # is there already a pending PUT?
                lock = self.pending.get_lock()
                lock.acquire()
                if self.pending[key]:
                    lock.release()
                    self.respond(conn, message_id, self.CANCEL_BYTEC,
                            self.EMPTY_BYTEC * 2)
                    value = int.from_bytes(req_bytes[5:7],
                            byteorder='big')
                else:
                    # mark it as pending
                    self.pending[key] = 1
                    lock.release()
                    value = int.from_bytes(req_bytes[5:7],
                            byteorder='big')
                    self.respond(conn, message_id, self.ACK_BYTEC,
                            self.EMPTY_BYTEC * 2)
            # need commit message before we can PUT value
            elif request_type == self.ACK_BYTEC:
                value = int.from_bytes(req_bytes[5:7], byteorder='big')
                self.table[key] = value
                self.pending[key] = 0
            elif request_type == self.CANCEL_BYTEC:
                self.pending[key] = 0
                value = int.from_bytes(req_bytes[5:7], byteorder='big')
            elif request_type == self.END_BYTEC:
                print("Workers aren't supposed to receive ENDs")
            else:
                print("Got bad request")

    def respond(self, conn, message_id, response_type, value):
        """Obtains a socket's write lock, then writes to the socket,
        blocking if necessary"""
        # figure out which client to send to
        client_id = int.from_bytes(message_id, byteorder='big') % self.num_clients
        # block until we get the write lock lock = self.sock_locks[client_id]
        lock = self.sock_locks[client_id]
        lock.acquire()
        conn.sendall(message_id + response_type + value)
        lock.release()
        if self.verbose:
            self.show_hex(message_id + response_type + value,
            prefix="Sending: ", use_outfile=True)

    def show_hex(self, data, prefix='', suffix='', use_outfile=False):
        """For debugging socket messages"""
        import textwrap
        data = textwrap.wrap(data.hex(), 2)
        if use_outfile:
            self.outfile.write(prefix + ' '.join(data) + suffix + '\n')
            self.outfile.flush()
        else:
            print(prefix + ' '.join(data) + suffix)

class Server(Process):
    # request types
    GET_BYTEC    = b'\x00'
    PUT_BYTEC    = b'\x01'
    END_BYTEC    = b'\x02'
    
    # response types
    EMPTY_BYTEC  = b'\x00'
    ACK_BYTEC    = b'\x06'
    CANCEL_BYTEC = b'\x18'

    # 2-phase-commit response types
    TWO_PC_BYTEC = (
        ACK_BYTEC,
        CANCEL_BYTEC
    )

    # regular (initial) requests
    GET_OR_PUT_BYTEC = (
        GET_BYTEC,
        PUT_BYTEC
    )
    def __init__(self, clients, server_host, config):
        # setup connection info
        self.hostname = socket.gethostname()
        self.clients = clients
        self.num_clients = len(clients)
        self.server_settings = (server_host, int(config['port']))
        self.backlog_size = int(config['backlog'])
        self.max_retries = int(config['max_retries'])

        # setup logging info
        self.verbose = config['verbose'].upper()[0] == 'T'
        outfilename = os.path.join(
                os.getenv("OUTPUT_DIR"),
                self.hostname + "_server.out")
        self.outfile = open(outfilename, 'w')

        # setup table
        num_keys = int(config['table_size'])
        # syncrhonized by means of an indicator array
        self.table = RawArray(c_int, num_keys)

        # setup worker synchronization
        self.num_workers = int(config['server_threads'])
        self.request_queue = Queue() # multi-producer/multi-consumer queues
        self.sock_locks = [Lock() for i in range(self.num_clients)]
        # a dumb globally-locked array keeps track of pending PUTs
        self.pending = Array(c_int, num_keys)

        # setup multiprocessing info
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
        """Waits for all nodes to connect"""
        s = self.socket
        connected = []
        clients = self.clients
        while len(connected) != len(clients):
            # block until someone is ready
            conn, addr = s.accept()
            connected.append(conn)
            self.outfile.write("received a connection from: " + repr(conn)\
                    + '\n')
            self.outfile.flush()

        self.outfile.write("Connected with {} clients{}".format(
            len(connected), os.linesep))
        self.outfile.flush()
        wlist = tuple()
        xlist = tuple()
        self.outfile.write("Reading messages" + os.linesep)
        self.outfile.flush()
        # start 8 worker threads
        pipes = []
        for worker_id in range(self.num_workers):
            parent_conn, child_conn = Pipe()
            pipes.append(parent_conn)
            Worker(worker_id, child_conn, s, connected, self.table,
                    self.request_queue, self.sock_locks, self.pending,
                    self.outfile, self.verbose).start()
        # keep track of the number of successful PUT operations
        num_puts = 0
        num_done = 0
        done = False
        while not done:
            # block until we get at least one message
            ready_list = select(connected, wlist, xlist)[0]
            for conn in ready_list:
                # read the message
                message = conn.recv(511) # multiple of 7 near 512
                if not message:
                    # we really shouldn't ever get here, but just in case
                    break
                # pass the message to a worker
                for line in self.split_multiline(message):
                    if self.verbose:
                        self.show_hex(line, prefix="Received: ",
                                use_outfile=True)
                        self.outfile.flush()
                    # is this is a commit/abort message?
                    request_type = line[2:3]
                    if request_type != self.END_BYTEC:
                        self.request_queue.put((line, conn.fileno()))
                    else:
                        # it's an end; record it
                        num_done += 1
                        message = "Received END #{}\n".format(num_done)
                        self.outfile.write(message)
                        self.outfile.flush()

    @staticmethod
    def respond(conn, message_id, response_type, data=b''):
        """Responds with bytecode for yes/no + any additional data
        that was requested by the client (if any)"""
        message_id = message_id.to_bytes(2, byteorder='big')
        if data != b'':
            data = data.to_bytes(2, byteorder='big')
        conn.sendall(message_id + response_type + data)

    def show_hex(self, data, prefix='', suffix='', use_outfile=False):
        """For debugging socket messages"""
        import textwrap
        data = textwrap.wrap(data.hex(), 2)
        if use_outfile:
            self.outfile.write(prefix + ' '.join(data) + suffix + '\n')
        else:
            print(prefix + ' '.join(data) + suffix)

    @staticmethod
    def split_multiline(data):
        """Returns the next parsed line of multiline input"""
        assert not len(data) % 7, "Got bad message length"
        while len(data):
            yield data[:7]
            data = data[7:]

    @staticmethod
    def parse_multiline(data):
        """Returns the next parsed line of multiline input"""
        assert not len(data) % 7, "Got bad message length"
        while len(data):
            message_id   = int.from_bytes(data[:2], byteorder='big')
            request_type = data[2:3]
            key          = int.from_bytes(data[3:5], byteorder='big')
            value        = int.from_bytes(data[5:7], byteorder='big')
            yield message_id, request_type, key, value
            data = data[7:]

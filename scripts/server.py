import os
import socket
from select import select
from sys import stdout
from multiprocessing import RawArray, Lock, Manager, Process, Pipe, Queue
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
            table_locks, request_queue, sock_locks):
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
        self.table_locks = table_locks
        # type: multiprocessing.Queue
        self.request_queue = request_queue
        # ensures two processes don't try to write to the same socket
        self.num_clients = len(sock_locks)
        self.sock_locks = sock_locks

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
                # can we acquire a lock?
                lock = self.table_locks[key % self.table_size]
                if lock.acquire(block=False):
                    # read value from table, convert it to ushort bytestring
                    value = self.table[key].to_bytes(2, byteorder='big')
                    self.respond(conn, message_id, self.ACK_BYTEC, value)
                else:
                    # let client know the location is locked
                    self.respond(conn, message_id, self.EMPTY_BYTEC,
                            self.EMPTY_BYTEC * 2)
            elif request_type == self.PUT_BYTEC:
                # need commit message before we can PUT value
                lock = self.table_locks[key % self.table_size]
                # is the lock busy?
                if lock.acquire(block=False):
                    # we own the lock
                    self.respond(conn, message_id, self.ACK_BYTEC,
                            self.WORKER_ID_BYTEC)
                    # translate value from bytes while we wait
                    value = int.from_bytes(req_bytes[5:7],
                            byteorder='big')
                    # block until we receive commit from main thread
                    self.pipe.poll(timeout=None)
                    message = self.pipe.recv_bytes(7)
                    action = message[2]
                    # only write change if user is committing
                    if action == self.ACK_BYTEC:
                        self.table[key] = value
                    # whether commit abort, we still need to unlock
                    lock.release()
                else:
                    self.respond(conn, message_id, self.CANCEL_BYTEC,
                            self.EMPTY_BYTEC * 2)
            elif request_type == self.END_BYTEC:
                # TODO END, probably just ignore
                pass

    def respond(self, conn, message_id, response_type, value):
        """Obtains a socket's write lock, then writes to the socket,
        blocking if necessary"""
        # figure out which client to send to
        client_id = int.from_bytes(message_id, byteorder='big') % self.num_clients
        # block until we get the write lock
        lock = self.sock_locks[client_id]
        lock.acquire()
        conn.sendall(message_id + response_type + value)
        lock.release()

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
    TWO_PC_BYTEC = [
        ACK_BYTEC,
        CANCEL_BYTEC
    ]
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
        #self.table = Table(100)
        # results in an array of type multiprocessing.sharedctypes.Array
        num_keys = 100
        self.table = RawArray(c_int, num_keys)
        self.locks = [Lock() for i in range(num_keys)]
        # multi-producer/multi-consumer queues
        self.request_queue = Queue()
        # ensures two processes don't try to write to the same socket
        self.sock_locks = [Lock() for i in range(self.num_clients)]

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

        self.outfile.write("Connected with {} clients{}".format(
            len(connected), os.linesep))
        self.outfile.flush()
        wlist = tuple()
        xlist = tuple()
        self.outfile.write("Reading messages" + os.linesep)
        self.outfile.flush()
        # start 8 worker threads
        pipes = []
        for worker_id in range(10):
            parent_conn, child_conn = Pipe()
            pipes.append(parent_conn)
            Worker(worker_id, child_conn, s, connected, self.table,
                    self.locks, self.request_queue,
                    self.sock_locks).start()
        # keep track of the number of successful PUT operations
        num_puts = 0
        num_done = 0
        done = False
        #self.pending = dict()
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
                    # is this is a commit/abort message?
                    request_type = line[2:3]
                    if request_type in self.TWO_PC_BYTEC:
                        # pass message to the thread that's waiting for it
                        worker_id = int.from_bytes(line[3:5],
                                byteorder='big')
                        pipes[worker_id].send_bytes(line, 0, 7)
                    else:
                        self.request_queue.put((line, conn.fileno()))

    @staticmethod
    def respond(conn, message_id, response_type, data=b''):
        """Responds with bytecode for yes/no + any additional data
        that was requested by the client (if any)"""
        message_id = message_id.to_bytes(2, byteorder='big')
        if data != b'':
            data = data.to_bytes(2, byteorder='big')
        conn.sendall(message_id + response_type + data)

    def show_hex(self, data, use_outfile=False):
        """For debugging socket messages"""
        import textwrap
        data = textwrap.wrap(data.hex(), 2)
        if use_outfile:
            self.outfile.write(' '.join(data) + '\n')
        else:
            print(' '.join(data))

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

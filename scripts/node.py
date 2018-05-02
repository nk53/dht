# Python STL
import socket
from sys import stdout
from multiprocessing import Pool
# custom
from client import Client
from server import Server

# static config options
IP_CONFIG = '../config/ips'
CONFIG = '../config/config.txt'

# read IP addresses from config file
with open(IP_CONFIG) as fh:
    host_list = [line.strip().split() for line in fh]

# read other configuration options
with open(CONFIG) as fh:
    options = dict([line.strip().split() for line in fh])


# attempt to connect to all hosts (except ourself); also handle requests
MAX_RETRIES = int(options['max_retries'])

#
# Setup Server/Client threads
#

# how many threads should we initialize?
SERVER_WORKERS = int(options['server_threads'])
# TODO: Unused
COORDINATOR_WORKERS = int(options['coordinator_threads'])

pool = Pool(SERVER_WORKERS + 1)

# settings for server end of sockets
SERVER_HOST = ''
SERVER_BACKLOG_SIZE = int(options['backlog'])

print("Starting server")
# connect by hostname, not IP
CLIENTS = list(zip(*host_list))[0]
server = Server(CLIENTS, SERVER_HOST, options)
server.start()
print("Server thread started")

# settings for client end of sockets
SERVERS = list(zip(*host_list))[0]

print("Starting client")
# connect by hostname, not IP
client = Client(SERVERS, options)
client.start()
print("Client thread started")
stdout.flush()

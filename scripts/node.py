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
SERVER_THREADS = int(options(['server_threads']))
COORDINATOR_THREADS = int(options['coordinator_threads'])

pool = Pool(SERVER_THREADS + 1)

# settings for server end of sockets
SERVER_HOST = ''
SERVER_PORT = int(options['port'])
SERVER_SETTINGS = (SERVER_HOST, SERVER_PORT)
SERVER_BACKLOG_SIZE = int(options['backlog'])

print("Starting server")
# connect by hostname, not IP
clients = list(zip(*host_list))[0]
server = Server(clients, SERVER_SETTINGS, SERVER_BACKLOG_SIZE,
        MAX_RETRIES)
server.start()
print("Server thread started")

# settings for client end of sockets
SERVERS = list(zip(*host_list))[0]
CLIENT_PORT = int(options['port'])
# package server list into tuples with: (hostname, port)
CLIENT_SETTINGS = list(zip(SERVERS, (CLIENT_PORT,) * len(SERVERS)))
CLIENT_BACKLOG_SIZE = int(options['backlog'])

print("Starting client")
# connect by hostname, not IP
client = Client(CLIENT_SETTINGS, CLIENT_BACKLOG_SIZE, MAX_RETRIES)
client.start()
print("Client thread started")
stdout.flush()

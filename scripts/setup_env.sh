# directories
export PROJECT_DIR=$HOME/dht
export CONFIG_DIR=$PROJECT_DIR/config
export OUTPUT_DIR=$PROJECT_DIR/output
export SCRIPT_DIR=$PROJECT_DIR/scripts
export TRANSACTION_DIR=$PROJECT_DIR/transactions

# configuration files
export SSH_ADDRESSES=$CONFIG_DIR/ssh_ips # hostnames according to SSH config
export ADDRESSES=$CONFIG_DIR/ips         # hostnames according to gethostname()
export HOST_LIST=$CONFIG_DIR/hosts
export SETTINGS=$CONFIG_DIR/config.txt

# scripts
export NODE_SCRIPT=$SCRIPT_DIR/node.py
export START_NODES=$SCRIPT_DIR/start_all.sh
export STOP_NODES=$SCRIPT_DIR/stop_all.sh
export KILL_THIS_PYTHON=$SCRIPT_DIR/kill_this_python.sh
export KILL_ALL_PYTHONS=$SCIPRT_DIR/kill_all_pythons.sh

# keeps track of running processes
export PIDS=$CONFIG_DIR/pids

# stored list of transactions, for testing purposes
# files named node_0, node_1, node_2, ...
export TRANSACTION_PREFIX=$TRANSACTION_DIR/node_

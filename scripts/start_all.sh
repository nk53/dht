#!/bin/bash

source setup_env.sh

if [ -e "$PIDS" ]; then
    echo "Error: nodes are already running"
    echo "First run $(basename $STOP_NODES), then retry"
    exit 1
fi

if [ -e ${ADDRESSES}_multi ]; then
    # we recently ran a single test; we need to undo the settings
    echo "Preparing for multi-node test ..."

    # the single-threaded tests usually take a while to release
    # the port/address binding (not sure why); so we'll just cycle them
    port_n=$(grep port $SETTINGS | sed "s/[^0-9]*//")
    if (( $port_n % 100 )); then
        let "port_n++"
    else
        let "port_n = port_n - 99"
    fi

    sed --in-place=".bak" "s/\(port[^0-9]*\)[0-9]*/\1$port_n/" $SETTINGS

    # swap single and multi files
    mv $ADDRESSES $ADDR_SINGLE
    mv ${ADDRESSES}_multi $ADDRESSES

    # sync config.txt with all nodes
    cp_all $SETTINGS
fi

hnames=($(awk '{print $1}' $SSH_ADDRESSES))

# log into each host, then start the node
rm $OUTPUT_DIR/*
for hname in ${hnames[@]}; do
    #echo "Starting $hname"
# begin SSH template
ssh -T -q $hname << TEMPLATE &
cd \$HOME/environments
source dht/bin/activate
cd $SCRIPT_DIR
rm $OUTPUT_DIR/*
source setup_env.sh
#echo "$NODE_SCRIPT 2> $OUTPUT_DIR/\$(hostname)_node.err > $OUTPUT_DIR/\$(hostname)_node.out"
python $NODE_SCRIPT 2> $OUTPUT_DIR/\$(hostname)_node.err > $OUTPUT_DIR/\$(hostname)_node.out &
echo "\$! : \$(hostname)" >> $PIDS
TEMPLATE
# end SSH template
done

./monitor.sh 3

./stop_all.sh > /dev/null

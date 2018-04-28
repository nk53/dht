#!/bin/bash

source setup_env.sh

# which python version is the default for this environment?
pyver=$(python -V)
py3ver=$(py3 -V)
if [ "$pyver" != "$py3ver" ]; then
    echo "Warning: Default Python version for this environment is $pyver"
    # we can still ensure we're using python version 3 in this shell ...
    cd $HOME/environments
    source dht/bin/activate
    cd $SCRIPT_DIR
fi

# remove output files
rm $OUTPUT_DIR/*

if [ -e ${ADDRESSES}_single ]; then
    # swap single with multi IP lists
    mv $ADDRESSES ${ADDRESSES}_multi
    mv $ADDR_SINGLE $ADDRESSES
fi

# the single-threaded tests usually take a while to release
# the port/address binding (not sure why); so we'll just cycle them
port_n=$(grep port $SETTINGS | sed "s/[^0-9]*//")
if (( $port_n % 100 )); then
    let "port_n++"
else
    let "port_n = port_n - 99"
fi

sed -i ".bak" "s/\(port[^0-9]*\)[0-9]*/\1$port_n/" $SETTINGS

# do single-node test
python node.py &

while [ $(./ends.sh 1 | wc -l) -lt 1 ] ; do
    sleep 1
done

echo Done

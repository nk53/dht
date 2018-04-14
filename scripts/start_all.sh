#!/bin/bash

source setup_env.sh

if [ -e "$PIDS" ]; then
    echo "Error: nodes are already running"
    echo "First run $(basename $STOP_NODES), then retry"
    exit 1
fi

rm $OUTPUT_DIR/*err
rm $OUTPUT_DIR/*out

hnames=($(awk '{print $1}' $SSH_ADDRESSES))

# log into each host, then start the node
for hname in ${hnames[@]}; do
    echo "Starting $hname"
# begin SSH template
ssh -T -q $hname << TEMPLATE &
dht
cd $SCRIPT_DIR
source setup_env.sh
echo "$NODE_SCRIPT 2> $OUTPUT_DIR/\$(hostname)_node.err > $OUTPUT_DIR/\$(hostname)_node.out"
python $NODE_SCRIPT 2> $OUTPUT_DIR/\$(hostname)_node.err > $OUTPUT_DIR/\$(hostname)_node.out &
echo "\$! : \$(hostname)" >> $PIDS
TEMPLATE
# end SSH template
done

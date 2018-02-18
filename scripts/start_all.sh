#!/bin/bash

source setup_env.sh

if [ -e "$PIDS" ]; then
    echo "Error: nodes are already running"
    echo "First run $(basename $STOP_NODES), then retry"
    exit 1
fi

rm $OUTPUT_DIR/*err
rm $OUTPUT_DIR/*out

# log into each host, then start the node
awk '{print $1}' $ADDRESSES | while read hname; do
    echo "Starting $hname"
# begin SSH template
ssh -T -q $hname << TEMPLATE &
cd $SCRIPT_DIR
source setup_env.sh
python $NODE_SCRIPT 2> $OUTPUT_DIR/\$(hostname).err > $OUTPUT_DIR/\$(hostname).out
echo "$! : \$(hostname)" >> $PIDS
TEMPLATE
# end SSH template
done

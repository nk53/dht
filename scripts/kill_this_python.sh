#!/bin/bash
# If you have to use this, you did something wrong. Shame on you.

# begin SSH template
PYTHON_PIDS=$(ps -u $(whoami) | grep -i python | awk '{print $1}')
if [ -n "$PYTHON_PIDS" ]; then
    echo "Processes targeted for termination on $(hostname): '$PYTHON_PIDS'"
    kill $PYTHON_PIDS
else
    echo "No pythons on $(hostname)"
fi

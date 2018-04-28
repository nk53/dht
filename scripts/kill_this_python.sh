#!/bin/bash
# If you have to use this, you did something wrong. Shame on you.

# begin SSH template
if [ $(uname) == "Darwin" ]; then
    # macOS
    PYTHON_PIDS=$(ps -u $(whoami) |
        grep -i python |
        grep -v grep | grep -v kill |
        awk '{print $2}')
else
    # most Linux distros
    PYTHON_PIDS=$(ps -u $(whoami) | grep -i python | grep -v grep | awk '{print $1}')
fi

if [ -n "$PYTHON_PIDS" ]; then
    echo "Processes targeted for termination on $(hostname):"
    echo $PYTHON_PIDS
    kill $PYTHON_PIDS
else
    echo "No pythons on $(hostname)"
fi

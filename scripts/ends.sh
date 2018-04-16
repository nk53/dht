#!/bin/bash
# Just a shortcut for checking whether run was successful

if [ -n "$1" ]; then
    ./show_output | grep "END #$1"
else
    ./show_output | grep "END #"
fi

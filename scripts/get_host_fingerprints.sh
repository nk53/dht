#!/bin/bash
# Adds all host fingerprints to the list of known hosts
# For more information, see:
# https://stackoverflow.com/questions/21383806/how-can-i-force-ssh-to-accept-a-new-host-fingerprint-from-the-command-line

source setup_env.sh

# location of hosts' RSA key
RSA_LOC=/etc/ssh/ssh_host_rsa_key.pub 
# location of our known_hosts file
KNOWN_HOSTS=~/.ssh/known_hosts

# prevents SSH from asking us to manually verify fingerprint
# do not use this more than once per host!
NO_CHECK="StrictHostKeyChecking no"

awk '{print $1}' $ADDRESSES | while read hname; do 
    echo "Checking $hname"
    ssh -q -T -o "$NO_CHECK" $hname << TEMPLATE
cat $RSA_LOC >> $KNOWN_HOSTS
TEMPLATE
done

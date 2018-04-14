#!/bin/bash

#
# Configuration settings
#

# name of the this project
PROJ_NAME=dht

# name of profile/bashrc file to add aliases
ALIAS_FILE=$HOME/.bashrc
ALIAS_CMD="alias dht='cd $HOME/environments; source dht/bin/activate; cd $HOME/dht'"

# directory to store virtual environments
ENV_DIR=$HOME/environments
# directory to store this project's environment
PROJ_ENV_DIR=$ENV_DIR/$PROJ_NAME
# directory to store the project itself
PROJ_DIR=$HOME/$PROJ_NAME

# make sure we're root
if [ "$(whoami)" == "root" ]; then
    echo "Can't run as root directly"
    exit 1
fi

sudo yum -y update
sudo yum -y install https://centos6.iuscommunity.org/ius-release.rpm
sudo yum -y install python36u
sudo yum -y install git

if [ -n "$(which python3.6)" ]; then
    # install pip for python3.6
    sudo yum -y install python36u-pip

    # make symlinks
    PY_INST_DIR=$(dirname $(which python3.6))
    PY_EXEC=$(which python3.6)
    sudo ln -s $PY_EXEC $PY_INST_DIR/python3
    sudo ln -s $PY_EXEC $PY_INST_DIR/py3

    # create virtual environment
    mkdir $ENV_DIR
    cd $ENV_DIR
    python3.6 -m venv $PROJ_NAME

    # add alias, if necessary
    if [ -z "$(grep "alias $PROJ_NAME" $ALIAS_FILE)" ]; then
        echo $ALIAS_CMD >> $ALIAS_FILE
    else
        echo "Alias for $PROJ_NAME already exists"
    fi

    # clone github repository
    if [ ! -e $HOME/dht ]; then
        cd $HOME
        git clone git@github.com:nk53/dht.git dht
    fi
fi

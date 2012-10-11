#!/bin/bash

GIT_PATH=`which git`
VIRTUALENV_PATH=`which virtualenv`

if [ "${GIT_PATH}" == "" or "${VIRTUALENV_PATH}" == "" ]; then
    echo "Install script require git and virtualenv"
    exit 1
fi

if [ $# -ne 2 ]; then
    echo "usage: install.sh <path>"
    exit 2
fi

PATH=$1

mkdir -p ${PATH}

`${VIRTUALENV_PATH} ${PATH}/env`
source ${PATH}/env/bin/activate

pip install lockfile
pip install netaddr
pip install netifaces
pip install psycopg2
pip install pycrypto
pip install python-daemon

cd ${PATH}

`${GIT_PATH} clone git://github.com/mprzytulski/pgherd.git`
cd pgherd

mkdir /etc/pgherd
cp ./pgherd/files/pgherd.conf /etc/pgherd/
cd ./pgherd/src
python pgherd.py install
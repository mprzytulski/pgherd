#!/usr/bin/env bash

export PATH="${PATH}:/usr/bin/"

GIT_PATH=`which git`
VIRTUALENV_PATH=`which virtualenv`

if [ "${GIT_PATH}" == "" ] || [ "${VIRTUALENV_PATH}" == "" ]; then
    echo "Install script require git and virtualenv"
    exit 1
fi

if [ $# -ne 1 ]; then
    echo "usage: install.sh <path>"
    exit 2
fi

INSTALL_PATH=$1

echo "Using git: ${GIT_PATH}"
echo "Using virtualenv: ${VIRTUALENV_PATH}"
echo "Install into: ${INSTALL_PATH}"

mkdir -p ${INSTALL_PATH}

if [ ! -d "${INSTALL_PATH}/env" ]; then

	${VIRTUALENV_PATH} "${INSTALL_PATH}/env"
	source ${INSTALL_PATH}/env/bin/activate

	pip install lockfile
	pip install netaddr
	pip install netifaces
	pip install psycopg2
	pip install pycrypto
	pip install python-daemon

fi

cd ${INSTALL_PATH}

if [ ! -f /etc/pgherd/env ]; then
    echo "PGHERD_INSTALL_DIR=\"${INSTALL_PATH}\"" > /etc/pgherd/env
fi

if [ ! -d ${INSTALL_PATH}/pgherd ]; then
	${GIT_PATH} clone git://github.com/mprzytulski/pgherd.git
else
	cd ${INSTALL_PATH}/pgherd
	${GIT_PATH} pull
	cd ${INSTALL_PATH}
fi

cd ./pgherd

if [ ! -d /etc/pgherd ]; then
	mkdir /etc/pgherd
	cp ${INSTALL_PATH}/pgherd/files/pgherd.conf /etc/pgherd/
fi

if [ ! -d /var/log/pgherd ]; then
    mkdir /var/log/pgherd
fi

if [ ! -d /var/run/pgherd ]; then
    mkdir /var/run/pgherd
fi

ln -sf ${INSTALL_PATH}/pgherd/files/pgherd /usr/sbin/pgherd
ln -sf ${INSTALL_PATH}/pgherd/files/pgherdd /usr/sbin/pgherdd
chmod +x /usr/sbin/pgherd
chmod +x /usr/sbin/pgherdd

cd ~/

echo "done"

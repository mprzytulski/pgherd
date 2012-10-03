__author__ = 'mike'

from threading import Thread

import psycopg2
import time
import logging

class Connections:

    master = None
    local = None

    def get_master(self):
        pass

    def get_local(self):
        pass

    def close(self):
        pass


class ConnectionMonitor(Thread):

    def __init__(self, event, config, connection):
        self._event = event
        self._config = config
        self._connection = connection
        super(ConnectionMonitor, self).__init__()

class LocalMonitor(ConnectionMonitor):
    pass


class MasterMonitor(ConnectionMonitor):
    pass

class Monitor(Thread):

    logger = logging.getLogger('default')

    def __init__(self, event, conf, discoverer):
        self._config = conf
        self._event = event
        self._discoverer = discoverer
        super(Monitor, self).__init__()

    def node_alive(self, event):
        pass

    def run(self):
        self.logger.info("Starting Monitor thread")
        self._connections = Connections(self._config)

        self._local = LocalMonitor(self._event, self._config, self._connections.get_local())
        self._master = MasterMonitor(self._event, self._config, self._connections.get_master())

        self._local.start()
        self._master.start()

        self._local.join()

        self._connections.close()
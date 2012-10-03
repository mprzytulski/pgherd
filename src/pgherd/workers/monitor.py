__author__ = 'mike'

from threading import Thread

import psycopg2
import time
import logging

class Monitor(Thread):

    logger = logging.getLogger('default')

    def __init__(self, event, conf):
        self._config = conf
        self._event = event
        super(Monitor, self).__init__()

    def node_alive(self, event):
        pass

    def run(self):
        while self._event.is_set():
            self.logger.debug('Monitor watch loop')
            time.sleep(self._config.interval)
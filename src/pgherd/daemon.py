__author__ = 'mike'

import time
import os
import logging

from pgherd.events import event
from pgherd.workers import Monitor
from pgherd.workers import Discoverer
from pgherd.workers import Negotiator
from pgherd.workers.monitor import Connections
from pgherd.workers.discoverer import Node

class Daemon(object):

    local_monitor  = None
    master_monitor = None
    discoverer = None
    negotiator = None

    logger = logging.getLogger('default')

    node = None
    connections = None

    def start(self, conf):

        self.node_name = os.uname()[1]

        self.connections = Connections(conf.monitor, self.node_name)

        status = None
        while status is None:
            status = (self.connections.get_local_status())
            if status is None:
                self.logger.debug("Wating for local node to startup")
                time.sleep(1)

        self.node = Node(*status)
        self.logger.info("Current node status: {}".format(self.node))

        self.discoverer = Discoverer(conf.discoverer)
        self.negotiator = Negotiator(conf.daemon)
#        daemon.monitor = Monitor(event, conf.monitor, daemon.discoverer)

        self.negotiator.start()
        self.discoverer.start()

        #while not self.discoverer.is_ready():
        #    time.sleep(0.5)


#        self.monitor.start()

daemon = Daemon()
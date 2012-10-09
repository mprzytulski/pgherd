__author__ = 'mike'

import time
import os
import logging
import socket

from pgherd.events import event
from pgherd.workers import Monitor
from pgherd.workers import Discoverer
from pgherd.workers import Negotiator
from pgherd.events import dispatcher
from pgherd.workers.negotiator import Node

class Daemon(object):

    local_monitor  = None
    master_monitor = None
    discoverer = None
    negotiator = None

    config = None

    logger = logging.getLogger('default')

    node = None
    connections = None

    def update_local_node_status(self, event):
        if self.node is None:
            self.node = Node(event.get_status())
        else:
            self.node.update(event.get_status())

    def start(self, conf):

        self.config = conf

        try:
            self.node_name = os.uname()[1]
            self.node_fqdn = socket.getfqdn(socket.gethostname())
            if conf.daemon.listen == '0.0.0.0':
                self.node_address = socket.gethostbyname(self.node_fqdn)
            else:
                self.node_address = conf.daemon.listen

        except:
            self.logger.exception("Failed getting node information")
            return 1

        dispatcher.addListener('monitor.local.update', self.update_local_node_status)

        self.discoverer = Discoverer(conf.discoverer)
        self.negotiator = Negotiator(conf.daemon)
        self.local_monitor = Monitor(conf.monitor, self.node_fqdn, conf.discoverer)

        self.negotiator.start()
        self.discoverer.start()
        self.local_monitor.start()

        #while not self.discoverer.is_ready():
        #    time.sleep(0.5)
        return 0


#        self.monitor.start()

daemon = Daemon()
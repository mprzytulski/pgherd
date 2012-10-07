__author__ = 'mike'

import subprocess
import logging

from pgherd.workers.discoverer import DiscovererPublisher, DiscovererMessage

class Command(object):

    _config = None
    logger = logging.getLogger('default')

    def __init__(self, config):
        self._config = config

    def run(self):
        raise RuntimeError("Unimplemented command")

class StartServer(Command):

    def run(self):
        subprocess.call(["/etc/init.d/postgresql-9.2", "start"])


class StopServer(Command):

    def run(self):
        subprocess.call(["/etc/init.d/postgresql-9.2", "stop"])

class ReloadServer(Command):

    def run(self):
        subprocess.call(["/etc/init.d/postgresql-9.2", "reload"])

class InitNode(Command):

    def _search_for_master(self, address, port):

        msg = DiscovererMessage('master.lookup')

        publisher = DiscovererPublisher(8766)
        publisher.send(msg)

        master_info = publisher.recv()

        print master_info

    def run(self):
        print "pgherd searching for cluster master node manager with broadcast port: {}".format(self._config.discoverer.port)
        self._search_for_master(self._config.discoverer.listen, self._config.discoverer.port)

class PromoteToMaster(Command):
    pass


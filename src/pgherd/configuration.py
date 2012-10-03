__author__ = 'mike'

import argparse
import ConfigParser

from netifaces import interfaces, ifaddresses, AF_INET

class Logging(object):
    level = None
    destination = None

class Daemon(object):
    listen = None
    port = None

class Monitor(object):
    host = 'localhost'
    port = 5432
    user = 'pgherd'
    password = 'pgherd'
    interval = 3
    timeout = 3
    attempts = 5

class Discoverer(object):
    auto = True
    port = 8766
    local_ips = []

class Commands(object):
    promote_to_master = 'internal'
    follow_master = 'internal'

class Configuration(object):

    daemonize = False
    config_file = None

    daemon = Daemon()
    monitor = Monitor()
    discoverer = Discoverer()
    logging = Logging()
    commands = Commands()

    def parse(self, argv):
        parser = argparse.ArgumentParser(prog="pgherd", description="Start pgherd process")
        parser.add_argument('-f', '--config', dest="config_file", default='/etc/pgherd/pgherd.conf', help="Configuration file path")
        parser.add_argument('-n', dest="demonize", default=False, action="store_false", help="Don't demonize process")
        parser.add_argument('--version', action='version', version='%(prog)s 0.1.0')
        parser.parse_args(argv, self)

        config = ConfigParser.ConfigParser()
        config.read(self.config_file)

        manual = {
            'daemon': ['port'],
            'monitor': ['port', 'interval', 'attempts', 'timeout'],
            'discoverer': ['port']
        }

        for section in config.sections():
            if hasattr(self, section):
                settings = getattr(self, section)
                items = config.items(section)
                for (key, val) in items:
                    if not hasattr(manual, section) or key not in manual[section]:
                        setattr(settings, key, val)
                setattr(self, section, settings)


        self.daemon.port = config.getint('daemon', 'port')

        self.monitor.port = config.getint('monitor', 'port')
        self.monitor.interval = config.getint('monitor', 'interval')
        self.monitor.attempts = config.getint('monitor', 'attempts')
        self.monitor.timeout = config.getint('monitor', 'timeout')

        self.discoverer.port = config.getint('discoverer', 'port')

        for ifaceName in interfaces():
            for i in ifaddresses(ifaceName).setdefault(AF_INET, [{'addr': None}]):
                if i['addr'] is not None:
                    self.discoverer.local_ips.append(i['addr'])

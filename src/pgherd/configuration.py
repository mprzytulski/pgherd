__author__ = 'mike'

import argparse
import ConfigParser
import os
import os.path
import sys
import socket

from netaddr import IPNetwork
from netifaces import interfaces, ifaddresses, AF_INET

class Logging(object):
    level = None
    destination = None

class Daemon(object):
    listen = None
    netmask = None
    port = None
    user = 'postgres'
    group = 'postgres'
    auth_key = None
    allow_pubkey_remote_store = True

class Archive(object):
    location = '/archive/'
    user = 'postgres'
    host = 'grunwald'
    mode = 'scp'

class Connection(object):
    host = 'localhost'
    port = 5432
    user = 'pgherd'
    password = 'pgherd'
    dbname = 'postgres'

class Monitor(Connection):
    interval = 3
    timeout = 3
    attempts = 5

class Discoverer(object):
    auto = True
    port = 8766
    listen = None
    local_ips = []
    broadcast = '<broadcast>'
    network = None

class Commands(object):
    promote_to_master = 'internal'
    follow_master = 'internal'
    rsync = None
    ssh = None
    scp = None

class Postgres(object):
    data_dir = '/var/lib/postgresql-9.2/data'
    conf_dir = '/etc/postgresql-9.2'
    archive_command = '/usr/sbin/pgherd archive %p %f'

class Replication(object):
    user = 'postgres'
    password = ''
    trigger_file = '/var/lib/postgresql/9.2/data/promote_to_master'
    restore_command = '/usr/sbin/pgherd restore %f %p'

class Configuration(object):

    daemonize = False
    config_file = None

    daemon = Daemon()
    monitor = Monitor()
    discoverer = Discoverer()
    logging = Logging()
    commands = Commands()
    replication = Replication()
    postgres = Postgres()
    archive = Archive()

    node_fqdn = None
    node_name = None

    def parse(self, argv, mode = 'daemon'):
        parser = argparse.ArgumentParser(prog="pgherd", description="Start pgherd process")
        parser.add_argument('-f', '--config', dest="config_file", default='/etc/pgherd/pgherd.conf', help="Configuration file path")
        parser.add_argument('--version', action='version', version='%(prog)s 0.1.0')

        if type(argv).__name__ == 'str':
            argv = argv.split(" ")

        if mode == 'daemon':
            parser.add_argument('-n', dest="demonize", default=False, action="store_false", help="Don't demonize process")
            parser.parse_args(argv, self)
        else:
            parser.parse_known_args(argv, self)

        self.node_name = os.uname()[1]
        self.node_fqdn = socket.getfqdn(socket.gethostname())

        if self.config_file is None:
            confs = ['/etc/pgherd/pgherd.conf', os.path.expanduser('~/.pgherd/pgherd.conf')]

            if os.path.exists(confs[0]):
                self.config_file = confs[0]

            elif os.path.exists(confs[1]):
                self.config_file = confs[1]

            else:
                print "Failed - no configuration file"
                sys.exit(1)

        config = ConfigParser.ConfigParser()
        config.read(self.config_file)

        manual = {
            'daemon': ['port', 'allow_pubkey_remote_store'],
            'monitor': ['port', 'interval', 'attempts', 'timeout'],
            'discoverer': ['port', 'network']
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
        self.daemon.allow_pubkey_remote_store = config.getboolean('daemon', 'allow_pubkey_remote_store')

        self.monitor.port = config.getint('monitor', 'port')
        self.monitor.interval = config.getint('monitor', 'interval')
        self.monitor.attempts = config.getint('monitor', 'attempts')
        self.monitor.timeout = config.getint('monitor', 'timeout')

        self.discoverer.port = config.getint('discoverer', 'port')
        self.discoverer.network = IPNetwork(config.get('discoverer', 'network'))
        self.discoverer.broadcast = str(self.discoverer.network.broadcast)

        for ifaceName in interfaces():
            for i in ifaddresses(ifaceName).setdefault(AF_INET, [{'addr': None}]):
                if i['addr'] is not None:
                    self.discoverer.local_ips.append(i['addr'])

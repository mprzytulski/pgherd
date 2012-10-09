__author__ = 'mike'

import subprocess
import logging
import socket
import cmd
import re
import os
import datetime

from pgherd.events import dispatcher
from pgherd.workers.discoverer import DiscovererMessage, discoverer_event_from_data

class Interpreter(cmd.Cmd):

    _config = None

    def __init__(self, config):
        self._config = config
        cmd.Cmd.__init__(self)
        self.prompt = '(pgheard) '
        self.intro  = "Welcome to pgherd! PostgreSQL Streaming Replication Cluster Manager"

    def do_init(self, args):
        """Initialize current node as part of cluster"""
        cmd = InitNode(self._config)
        cmd.run()

    def do_start(self, args):
        pass

    def do_stop(self, args):
        pass

    def do_reload(self, args):
        pass

    def do_status(self, args):
        """Display current cluster status"""
        pass

    def do_recovery(self, args):
        """Recovery current node"""

    def do_backup(self, args):
        """Backup current nod to file"""

    def do_exit(self, args):
        """Exits from the console"""
        print "\n"
        return -1

    def do_EOF(self, args):
        """Exit on system end of file character"""
        return self.do_exit(args)


class Command(object):

    _config = None
    logger = logging.getLogger('default')

    def __init__(self, config):
        self._config = config
        dispatcher.addListener('discoverer.message.receive.master.info', self.handle_response)

    def handle_response(self, event):
        pass

    def discovery_request(self, msg):

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(5)

        sock.sendto(str(msg), (self._config.discoverer.broadcast, self._config.discoverer.port))

        received = sock.recv(1024)
        discoverer_event_from_data(received)

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

    def postgres_conf_listen_addresses(self, val):
        return "listen_addresses = '*'"

    def postgres_wal_level(self, val):
        return "wal_level = 'hot_standby'"

    def postgres_wal_senders(self, val):
        return "max_wal_senders = 5"

    def postgres_wal_keep_segments(self, val):
        return "wal_keep_segments = 32"

    def postgres_archive_mode(self, val):
        return "archive_mode = 'on'"

    def postgres_archive_command(self, val):
        return "archive_command = '{}'".format(self._config.postgres.archive_command)

    def postgres_hot_standby(self, val):
        return "hot_standby = on"

    def parse_postgres_conf(self, file_name):

        if not os.path.exists(file_name):
            raise Exception("Invalid postgresql configuration file: '{}'".format(file_name))

        lre = re.compile("(\#|)([^=]*)=([^\#]*)")
        new_config = []
        with open(file_name, 'r') as file:
            for line in file:
                matches = lre.match(line)
                param = matches.group(2)
                name = 'postgres_conf_{}'.format(param.strip())
                if hasattr(self, name):
                    to_call = getattr(self, name)
                    val = matches.group(3).strip()
                    line = "{} # org: {} = {}".format(to_call(val), param, val)

                new_config.append(line)

        new_name = file_name + datetime.datetime.now().strftime("%Y%m%d%H%M")
        print "Rename: '{}' to '{}'... ".format(file_name, new_name)
        os.rename(file_name, new_name)
        print "done\n"

        print "Writing new {}... ".format(file_name)
        with open(file_name, 'w+') as file:
            file.write(new_config)
        print "done\n"

    def write_recovery_conf(self, file_name, master):

        print "Creating {}... ".format(file_name)
        content = """
        $ $EDITOR recovery.conf
        # Note that recovery.conf must be in $PGDATA directory.

        standby_mode          = 'on'
        primary_conninfo      = 'host={} port={} user={} password={}'
        trigger_file          = '{}'
        restore_command       = '{}'
        """.format(master.get("name"), master.get("5432"),
            self._config.replication.user, self._config.replication.password,
            self._config.replication.trigger_file, self._config.replication.restore_command)
        try:
            file = open(file_name, 'w+')
            file.write(content)
            file.close()
        except:
            raise Exception("Filed creating recovery file: '{}'".format(file_name))
        print "done\n"


    def handle_response(self, event):
        resp = ''
        allowed = ['yes', 'no']
        while resp not in allowed:
            resp = raw_input("Master db found at: {}:{} connect replication? YES|no "
                .format(event.get_message().get("node_name"), event.get_message().get('port'))).lower()
            if resp.strip() == "":
                resp = 'yes'

        if resp == 'yes':
            try:
                self.parse_postgres_conf(self._config.postgres.conf_dir + "/postgresql.conf")
                self.write_recovery_conf(self._config.postgres.data_dir + "/recovery.conf", event.get_message())
            except Exception, e:
                print e

    def _search_for_master(self):

        msg = DiscovererMessage('master.lookup')
        try:
            self.discovery_request(msg)
        except socket.timeout:
            raise

    def run(self):
        print "pgherd searching for cluster master node manager with broadcast port: {}".format(self._config.discoverer.port)
        self._search_for_master()

class PromoteToMaster(Command):
    pass


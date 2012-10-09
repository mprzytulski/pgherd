__author__ = 'mike'

import subprocess
import logging
import socket
import cmd
import re
import os
import datetime
import sys
import getpass

from distutils.version import StrictVersion
from pgherd.events import dispatcher
from pgherd.configuration import Connection
from pgherd.workers.monitor import Connections
from pgherd.workers.discoverer import DiscovererMessage, discoverer_event_from_data, Event
from pgherd.libs.table import Table

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
        cmd = ClusterStatus(self._config)
        cmd.run()

    def do_recovery(self, args):
        """Recovery current node"""
        cmd = Recovery(self._config)
        cmd.run()

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

    def add_listener(self, event):
        dispatcher.addListener(event, self.handle_response)
        dispatcher.addListener('command.end_of_data', self.end_of_data)

    def handle_response(self, event):
        pass

    def end_of_data(self, event):
        pass

    def _ask(self, question, allowed):
        resp = ''
        allowed = ['yes', 'no']
        while resp not in allowed:
            resp = raw_input(question).lower()
            if resp.strip() == "":
                resp = 'yes'
        return resp

    def discovery_request(self, msg):

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(5)

        sock.sendto(str(msg), (self._config.discoverer.broadcast, self._config.discoverer.port))

        while True:
            received = None
            try:
                received = sock.recv(1024)
            except socket.timeout:
                pass
            if received is not None:
                discoverer_event_from_data(received)
            else:
                break

        dispatcher.notify('command.end_of_data', Event())

    def run(self):
        raise RuntimeError("Unimplemented command")

class StartServer(Command):

    def run(self):
        sys.stdout.write("Starting PostgreSQL server... ")
        subprocess.call(["/etc/init.d/postgresql-9.2", "start"])
        sys.stdout.write("done\n")


class StopServer(Command):

    def run(self):
        subprocess.call(["/etc/init.d/postgresql-9.2", "stop"])

class ReloadServer(Command):

    def run(self):
        subprocess.call(["/etc/init.d/postgresql-9.2", "reload"])

class Recovery(Command):

    def _rsync_data(self, path):
        pass

    def handle_response(self, event):

        conf = Connection()
        conf.host = event.get("listen")[0]
        conf.port = event.get("host")
        conf.dbname = self._config.monitor.dbname
        conf.user = self._config.monitor.user
        conf.password = self._config.monitor.password

        connections = Connections(self._config.discoverer)
        connections.set_master(conf, event.get_message().get("node_name"))

        try:
            cursor = connections.get_master()
            version = StrictVersion(event.get_message().get('version'))
            if version >= StrictVersion('9.2'):
                sql = "SELECT pg_tablespace_location(oid) spclocation FROM pg_tablespace " \
                      "WHERE spcname NOT IN ('pg_default', 'pg_global');"
            elif version < StrictVersion('9.2'):
                sql = "SELECT spclocation FROM pg_tablespace " \
                      "WHERE spcname NOT IN ('pg_default', 'pg_global');"
            cursor.execute(sql)

            tablespaces = cursor.fetchall()
            tablespaces.append(self._config.postgres.data_dir)

            cursor.execute("select pg_start_backup('pgherd backup {}')".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))



        except:
            pass


        # get_tablespaces
        # pg_start_backup()
        # rsync data
        # pg_stop_backup
        # reconfigure
        # start_node
        pass


    def end_of_data(self, event):
        pass

    def run(self):
        self.add_listener('discoverer.message.receive.master.info')
        msg = DiscovererMessage('master.lookup')
        try:
            self.discovery_request(msg)
        except socket.timeout:
            raise

class ClusterStatus(Command):

    _statuses = []

    def handle_response(self, event):
        self._statuses.append(event.get_message())

    def end_of_data(self, event):
        table_data = (("Name", "IP:port", "Master", "Recovery", "Replica lag"),)
        for status in self._statuses:
            print status.get("node_name")
            table_data = table_data + ((status.get("node_name"), status.get("listen")[0] + ":" + status.get("port"),
                str(status.get("is_master")), str(status.get("is_recovery")), str(status.get("x_log_location"))),)
        table = Table(table_data)
        print table.create_table()

    def run(self):
        self.add_listener('discoverer.message.receive.cluster.status')
        msg = DiscovererMessage('cluster.status')
        try:
            self.discovery_request(msg)
        except socket.timeout:
            raise

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
                if matches:
                    param = matches.group(2)
                    name = 'postgres_{}'.format(param.strip())
                    if hasattr(self, name):
                        to_call = getattr(self, name)
                        val = matches.group(3).strip()
                        nline = to_call(val)
                        if nline != line.strip():
                            line = "{}\n#org: {}".format(nline, line)


                new_config.append(line)

        new_name = file_name + datetime.datetime.now().strftime("-%Y%m%d%H%M")
        sys.stdout.write("Rename: '{}' to '{}'... ".format(file_name, new_name))
        os.rename(file_name, new_name)
        sys.stdout.write("done\n")

        sys.stdout.write("Writing new {}... ".format(file_name))
        with open(file_name, 'w+') as file:
            for item in new_config:
                file.write(item)

        sys.stdout.write("done\n")

    def write_recovery_conf(self, file_name, master):

        sys.stdout.write("Creating {}... ".format(file_name))
        content = "$ $EDITOR recovery.conf\n" \
                  "# Note that recovery.conf must be in $PGDATA directory.\n" \
                  "standby_mode          = 'on'\n" \
                  "primary_conninfo      = 'host={} port={} user={} password={}'\n" \
                  "trigger_file          = '{}'\n" \
                  "restore_command       = '{}'\n".format(master.get("listen")[0], master.get("port"),
            self._config.replication.user, self._config.replication.password,
            self._config.replication.trigger_file, self._config.replication.restore_command)
        try:
            file = open(file_name, 'w+')
            file.write(content)
            file.close()
        except:
            raise Exception("Filed creating recovery file: '{}'".format(file_name))

        sys.stdout.write("done\n")

    def _generate_key(self):
        user = getpass.getuser()
        keypath = os.path.expanduser('~{}/.ssh'.format(self._config.daemon.user))

        if not os.path.isdir(keypath):
            os.mkdir(keypath)



    def handle_response(self, event):

        resp = self._ask("Master db found: {} at: {}:{} connect replication? YES|no "
            .format(event.get_message().get("node_name"), event.get_message().get('listen')[0],
                event.get_message().get('port')), ['yes', 'no'])
        if resp == 'yes':
            try:
                self.parse_postgres_conf(self._config.postgres.conf_dir + "/postgresql.conf")
                self.write_recovery_conf(self._config.postgres.data_dir + "/recovery.conf", event.get_message())
            except Exception, e:
                print e

            resp = self._ask("Start node? ", ['yes', 'no'])
            if resp == 'yes':
                StartServer(self._config).run()


    def run(self):
        self.add_listener('discoverer.message.receive.master.info')
        msg = DiscovererMessage('master.lookup')
        try:
            self.discovery_request(msg)
        except socket.timeout:
            raise

class PromoteToMaster(Command):
    pass


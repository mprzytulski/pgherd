__author__ = 'mike'

import subprocess
import logging
import socket
import cmd
import re
import os
import datetime
import sys
import pwd
import getpass
import stat

from Crypto.PublicKey import RSA
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

    def do_promote(self, args):
        """Promote current node to master role"""
        cmd = PromoteToMaster(self._config)
        cmd.run()

    def do_gen_key(self, args):
        """Generate RSA key pair"""
        cmd = GenerateKey(self._config)
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

    def get_home_dir(self):
        try:
            pw = pwd.getpwnam(self._config.daemon.user)
        except KeyError:
            raise RuntimeError("Invalid user: {}".format(self._config.daemon.user))
        return os.path.expanduser('~/{}/'.format(self._config.daemon.user))

    def get_hostname(self):
        return os.uname()[1]

    def handle_response(self, event):
        pass

    def end_of_data(self, event):
        pass

    def _ask(self, question, allowed, default = 'yes'):
        resp = ''
        allowed = ['yes', 'no']
        while resp not in allowed:
            resp = raw_input(question).lower()
            if resp.strip() == "":
                resp = default
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

class GenerateKey(Command):

    def run(self, force = False):
        key_dir = self.get_home_dir() + '/.ssh/'
        if not os.path.isdir(key_dir) or force:
            sys.stdout.write("Generate RSA key pair... ")
            key = RSA.generate(2048, os.urandom)
            f = open(key_dir + 'id_rsa','w')
            f.write(key.exportKey('PEM'))
            f.close()

            try:
                os.chown(key_dir + 'id_rsa', self._config.daemon.user, self._config.daemon.group)
                os.chmod(key_dir + 'id_rsa', stat.S_IRUSR)
            except:
                pass


            f = open(key_dir + 'id_rsa.pub', 'w')
            f.write(key.exportKey('OpenSSH') + " {}@{}".format(self._config.daemon.user, self._config))

            try:
                os.chown(key_dir + 'id_rsa.pub', self._config.daemon.user, self._config.daemon.group)
                os.chmod(key_dir + 'id_rsa.pub', stat.S_IRUSR)
            except:
                pass

            sys.stdout.write("done\n")
        else:
            print "Using existing RSA key pair"

        f = open(key_dir + 'id_rsa.pub', 'r')
        key = f.read()
        f.close()
        print key

class Recovery(Command):

    def _rsync_data(self, path, master_host):
        sys.stdout.write("rsync data: {}... ".format(path))
        rsync_opts = "--exclude=pg_xlog* --exclude=pg_control --exclude=*.pid {}@{} {}".format(
            self._config.daemon.user, master_host, path
        )
        self.logger.info("Call rsync: {} {}".format(self._config.commands.rsync, rsync_opts))
        subprocess.call(self._config.commands.rsync, rsync_opts)
        sys.stdout.write("done\n")

    def _reconfigure_node(self):
        sys.stdout.write("Updating configuration files... ")
        cmd = InitNode(self._config)
        cmd.run()
        sys.stdout.write("done\n")

    def _start_node(self):
        sys.stdout.write("Starting node... ")
        pass
        sys.stdout.write("done\n")

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
            sys.stdout.write("Connecting to masterdb... ")
            cursor = connections.get_master()
            sys.stdout.write("done\n")
            sys.stdout.write("Getting information about tablespace... ")
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
            sys.stdout.write("done\n")

            sys.stdout.write("Starting PostgreSQL backup\n")
            cursor.execute("select pg_start_backup('pgherd backup {}')".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))

            for tablespace in tablespaces:
                self._rsync_data(tablespace, conf.host)

            cursor.execute("select pg_stop_backup()")
            sys.stdout.write("PostgreSQL backup done\n")
            cursor.close()

            self._reconfigure_node()
            self._start_node()

        except:
            pass

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

    _slave = False

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

    def _create_postgres_conf(self, file_name):

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

        os.chown(file_name, 'postgres', 'postgres')
        sys.stdout.write("done\n")

    def _create_recovery_conf(self, file_name, master):

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

            os.chown(file_name, 'postgres', 'postgres')
        except:
            raise Exception("Filed creating recovery file: '{}'".format(file_name))

        sys.stdout.write("done\n")

    def _start_node(self):
        resp = self._ask("Start node? [YES|no] ", ['yes', 'no'])
        if resp == 'yes':
            StartServer(self._config).run()


    def _init_master(self):
        resp = self._ask("No master node found init current as master? YES|no ", ['yes', 'no'])
        if resp == 'yes':
            try:
                try:
                    os.remove(self._config.postgres.data_dir + "/recovery.conf")
                except:
                    pass
                try:
                    os.remove(self._config.replication.trigger_file)
                except:
                    pass
                self._create_postgres_conf(self._config.postgres.conf_dir + "/postgresql.conf")
            except Exception, e:
                print e

            self._start_node()

    def _init_slave(self, event):
        resp = self._ask("Master db found: {} at: {}:{} connect replication? YES|no "
            .format(event.get_message().get("node_name"), event.get_message().get('listen')[0],
                event.get_message().get('port')), ['yes', 'no'])
        if resp == 'yes':
            try:
                self._create_postgres_conf(self._config.postgres.conf_dir + "/postgresql.conf")
                self._create_recovery_conf(self._config.postgres.data_dir + "/recovery.conf", event.get_message())
            except Exception, e:
                print e

            self._start_node()

    def handle_response(self, event):
        self._slave = True
        self._init_slave(event)

    def end_of_data(self, event):
        if not self._slave:
            self._init_master()

    def run(self):
        self.add_listener('discoverer.message.receive.master.info')
        msg = DiscovererMessage('master.lookup')
        self.discovery_request(msg)


class PromoteToMaster(Command):

    def run(self):
        sys.stdout.write("Creating trigger file at: {}... ".format(self._config.replication.trigger_file))
        try:
            f = open(self._config.replication.trigger_file, 'w')
            f.close()
            sys.stdout.write("done\n")
        except:
            sys.stdout.write("fail\n")


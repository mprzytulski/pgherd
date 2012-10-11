__author__ = 'mike'

import psycopg2
import time
import logging
import json
import socket

from netaddr import IPAddress, AddrFormatError
from threading import Thread
from pgherd.events import event, dispatcher, Event
from psycopg2.extras import LoggingConnection, RealDictCursor

class Connections:

    _master = None
    _master_config = None
    _master_node_name = None

    _local = None
    _local_config = None
    _local_node_name = None
    _discoverer = None
    _local_ips = None

    logger = logging.getLogger('default')

    def __init__(self, discoverer_config):
        self._discoverer = discoverer_config

    def set_local(self, local_config, local_node_name):
        self._local_config = local_config
        self._local_node_name = local_node_name

    def set_master(self, master_config, master_node_name):
        self._master_config = master_config
        self._master_node_name = master_node_name

    def _get_listen_addresses(self, cursor):
        if self._local_ips is not None:
            return self._local_ips

        try:
            ips = ','.join(self._discoverer.local_ips)
            sql = "SELECT case current_setting('listen_addresses') " \
                  "when '*' then '{}' "\
                  "else current_setting('listen_addresses') end as listen ".format(ips, ips)

            cursor.execute(sql, (self._local_node_name,))
            data = cursor.fetchone()
            listen = data['listen'].split(',')
            ips = []
            for addr in listen:
                try:
                    ip_addr = IPAddress(addr)
                except AddrFormatError:
                    addr = socket.gethostbyname(addr)
                    ip_addr = IPAddress(addr)

                if ip_addr in self._discoverer.network:
                    ips.append(addr)

            if len(ips) == 0:
                raise RuntimeError("Invalid PostgreSQL Server configuration no bind to cluster network: [{}] listen on: {}"
                    .format(self._discoverer.network, listen))

            self._local_ips = ips
            return ips
        except psycopg2.OperationalError, psycopg2.InternalError:
            self._local_ips = None
            self.logger.exception("Broken connection reconnecting")

    def _get_status(self, cursor):
        try:
            #"pg_last_xact_replay_timestamp() as xlog_time, " \
            sql = "SELECT " \
                  "%s as node_name, "\
                  "current_setting('port') as port, "\
                  "pg_last_xlog_replay_location() as xlog_location,"\
                  "current_setting('server_version') as version, " \
                  "pg_is_in_recovery() as is_recovery, "\
                  "case current_setting('hot_standby') when 'off' then true else false end as is_master"
            cursor.execute(sql, (self._local_node_name,))
            data = cursor.fetchone()
            data['listen'] = self._get_listen_addresses(cursor)
            return data
        except psycopg2.OperationalError, psycopg2.InternalError:
            self._local = None
            self.logger.exception("Broken connection reconnecting")
        except psycopg2.ProgrammingError:
            self.logger.exception("Failed updating local node status")

    def _get_connection(self, dbname, username, password, host, port):
        conn = None
        try:
            conn =  psycopg2.connect(database=dbname, user=username, password=password,
                host=host, port=port)
            #, connection_factory = LoggingConnection

            #self._local.initialize(self.logger)

            conn.autocommit = True
        except psycopg2.OperationalError, psycopg2.InternalError:
            err = "Failed connecting node {}:{}/{} with user: '{}'".format(
                host,
                port,
                dbname,
                username
            )
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.exception(err)
            else:
                self.logger.info(err)
        finally:
            return conn

    def get_master(self):
        if self._master is None:
            self._master = self._get_connection(self._master_config.dbname, self._master_config.user,
                self._master_config.password, self._master_config.host, self._master_config.port)

        return self._master.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def get_master_status(self):
        return self._get_status(self.get_master())

    def get_local(self):
        if self._local is None:
            self._local = self._get_connection(self._local_config.dbname, self._local_config.user,
                self._local_config.password, self._local_config.host, self._local_config.port)

        return self._local.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def get_local_status(self):
        return self._get_status(self.get_local())

    def close(self):

        local = self.get_local()
        if local:
            local.close()

        master = self.get_master()
        if master:
            master.close()


class StatusEvent(Event):

    def __init__(self, status):
        self._status = status

    def get_status(self):
        return self._status

    def __str__(self):
        return json.dumps(self._status)

class ConnectionMonitor(Thread):

    _config = None
    _connections = None
    logger = logging.getLogger('default')

    def __init__(self, config, connections):
        self._config = config
        self._connections = connections

        super(ConnectionMonitor, self).__init__()

class LocalMonitor(ConnectionMonitor):

    def run(self):

        self.logger.debug('Starting monitoring local node')
        while event.is_set():
            try:
                status = StatusEvent(self._connections.get_local_status())
                self.logger.debug('Get local node status: {}'.format(status))
                dispatcher.notify('monitor.local.update', status)
            except:
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.exception("Failed getting local node status - waiting for node wake up ")
                else:
                    self.logger.warning("Failed getting local node status - waiting for node wake up ")
            finally:
                time.sleep(self._config.interval)



class MasterMonitor(ConnectionMonitor):

    def run(self):
        self.logger.debug('Starting monitoring master node')
        while event.is_set():
            status = self._connections.get_local_status()
            self.logger.debug('Get master node status: {}'.format(status))
            time.sleep(self._config.interval)

class Monitor(Thread):

    logger = logging.getLogger('default')
    _config = None
    _node_fqdn = None
    _discoverer = None

    def __init__(self, conf, fqdn, discoverer):
        self._config = conf
        self._node_fqdn = fqdn
        self._discoverer = discoverer
        super(Monitor, self).__init__()

    def node_alive(self, event):
        pass

    def run(self):
        self.logger.info("Starting Monitor thread")

        self._connections = Connections(self._discoverer)
        self._connections.set_local(self._config, self._node_fqdn)

        self._local = LocalMonitor(self._config, self._connections)
#        self._master = MasterMonitor(self._config, self._connections)

        self._local.start()
#        self._master.start()

        self._local.join()

        self._connections.close()
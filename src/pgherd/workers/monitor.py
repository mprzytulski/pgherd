__author__ = 'mike'

import psycopg2
import time
import logging
import json

from threading import Thread
from pgherd.events import event, dispatcher, Event
from psycopg2.extras import LoggingConnection

class Connections:

    _master = None
    _master_config = None

    _local = None
    _local_config = None
    _local_node_name = None

    logger = logging.getLogger('default')

    def __init__(self, local_config, local_node_name):
        self._local_config = local_config
        self._local_node_name = local_node_name

    def _get_status(self, connection):
        try:
            cursor = connection.cursor()
            #"pg_last_xact_replay_timestamp() as xlog_time, " \
            sql = "SELECT %s as node_name, pg_last_xlog_replay_location() as xlog_location,"\
                  "version() as version, "\
                  "pg_is_in_recovery() as is_recovery, "\
                  "case current_setting('hot_standby') when 'off' then true else false end as is_master"
            cursor.execute(sql, (self._local_node_name,))
            return cursor.fetchone()
        except psycopg2.OperationalError, psycopg2.InternalError:
            self._local = None
            self.logger.exception("Broken connection reconnecting")
        except psycopg2.ProgrammingError:
            self.logger.exception("Failed updating local node status")

    def get_master(self):
        pass

    def get_master_status(self):
        return self._get_status(self.get_master())

    def get_local(self):
        if self._local is None:
            try:
                self._local = psycopg2.connect(database=self._local_config.dbname, user=self._local_config.user, password=self._local_config.password,
                    host=self._local_config.host, port=self._local_config.port)
                #, connection_factory = LoggingConnection

                #self._local.initialize(self.logger)

                self._local.autocommit = True
            except psycopg2.OperationalError, psycopg2.InternalError:
                self._local = None
                err = "Failed connecting node {}:{}/{} with user: '{}'".format(
                    self._local_config.host,
                    self._local_config.port,
                    self._local_config.dbname,
                    self._local_config.user
                )
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.exception(err)
                else:
                    self.logger.info(err)
                raise

        return self._local

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

    def __init__(self, conf, fqdn):
        self._config = conf
        self._node_fqdn = fqdn
        super(Monitor, self).__init__()

    def node_alive(self, event):
        pass

    def run(self):
        self.logger.info("Starting Monitor thread")
        self._connections = Connections(self._config, self._node_fqdn)

        self._local = LocalMonitor(self._config, self._connections)
        self._master = MasterMonitor(self._config, self._connections)

        self._local.start()
        self._master.start()

        self._local.join()

        self._connections.close()
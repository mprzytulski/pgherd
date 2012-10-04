__author__ = 'mike'

import psycopg2
import time
import logging

from threading import Thread
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

    def get_master(self):
        pass

    def get_master_status(self):
        pass

    def get_local(self):
        if self._local is None:

            self._local = psycopg2.connect(database=self._local_config.dbname, user=self._local_config.user, password=self._local_config.password,
                host=self._local_config.host, port=self._local_config.port, connection_factory = LoggingConnection)

            self._local.initialize(self.logger)

            self._local.autocommit = True

        return self._local

    def get_local_status(self):
        try:
            cursor = self.get_local().cursor()
            #"pg_last_xact_replay_timestamp() as xlog_time, " \
            sql = "SELECT %s as node_name, pg_last_xlog_replay_location() as xlog_location, " \
                  "pg_is_in_recovery() as is_recovery, " \
                  "case current_setting('hot_standby') when 'off' then true else false end as is_master"
            cursor.execute(sql, (self._local_node_name,))
            return cursor.fetchone()
        except psycopg2.OperationalError, psycopg2.InternalError:
            self._local = None
            self.logger.exception("Broken connection reconnecting")
        except psycopg2.ProgrammingError:
            self.logger.exception("Failed updating local node status")

    def close(self):
        pass



class ConnectionMonitor(Thread):

    def __init__(self, event, config, connection):
        self._event = event
        self._config = config
        self._connection = connection
        super(ConnectionMonitor, self).__init__()

class LocalMonitor(ConnectionMonitor):
    pass


class MasterMonitor(ConnectionMonitor):
    pass

class Monitor(Thread):

    logger = logging.getLogger('default')

    def __init__(self, event, conf, discoverer):
        self._config = conf
        self._event = event
        self._discoverer = discoverer
        super(Monitor, self).__init__()

    def node_alive(self, event):
        pass

    def run(self):
        self.logger.info("Starting Monitor thread")
        self._connections = Connections(self._config)

        self._local = LocalMonitor(self._event, self._config, self._connections.get_local())
        self._master = MasterMonitor(self._event, self._config, self._connections.get_master())

        self._local.start()
        self._master.start()

        self._local.join()

        self._connections.close()
__author__ = 'mike'


import logging
import json
import SocketServer
import socket
import time

from threading import Thread
from pgherd.events import event
from pgherd.events import dispatcher
from pgherd.workers.negotiator.messages import NodeStatus

class Node(object):

    _name = None
    _x_log_location = 0
    _is_recovery = False
    _is_master = None
    _version = None

    def __init__(self, name = "", xlog_location = 0, version = 0, is_recovery = False, is_master = False):
        self.update(name, xlog_location, version, is_recovery, is_master)

    def update(self, name = "", xlog_location = 0, version = 0, is_recovery = False, is_master = False):
        self._name = name
        self._x_log_location = self.__xlog_to_bytes(xlog_location)
        self._version = version
        self._is_recovery = is_recovery
        self._is_master = is_master


    def get_name(self):
        return self._name

    def is_master(self):
        return self._is_master

    def as_dict(self):
        return {'name': self._name, 'x_log_location': self._x_log_location,
                'version': self._version,
                'is_recovery': self._is_recovery, 'is_master': self._is_master}

    def __str__(self):
        return json.dumps(self.as_dict())

    def __xlog_to_bytes(self, xlog):
        """
        Convert an xlog number like '0/C6321D98' to an integer representing the
        number of bytes into the xlog.

        Logic here is taken from
        https://github.com/mhagander/munin-plugins/blob/master/postgres/postgres_streaming_.in.
        I assume it's correct...
        """
        if xlog is None:
            return 0
        logid, offset = xlog.split('/')
        return (int('ffffffff', 16) * int(logid, 16)) + int(offset, 16)

class Nodes(object):

    _nodes = {}
    _master = None

    def add(self, node):
        if not self.nodes.has_key(node.get_name()):
            self._nodes[node.get_name()] = node
            if node.is_master():
                self._master = node.get_name()

    def get_master(self):
        if self._master is not None:
            return self._nodes[self._master]
        else:
            return None

class TcpRequestHandler(SocketServer.StreamRequestHandler):

    def send(self, message):
        self.wfile.write(str(message))

    def read(self):
        message = json.loads(self.rfile.readline().strip())
        return message

    def handle(self):
        from pgherd.daemon import  daemon
        try:
            self.logger = logging.getLogger('default')

            if daemon.node is not None:
                self.send(NodeStatus(daemon.node))

            while event.is_set():
                self.logger.debug('Negotiator wating for message')
                message = self.read()
                dispatcher.notify('negotiator.message.receive', message)

        except Exception, ex:
            self.logger.exception("Unexpected error:")
        finally:
            self.request.close()


class NegotiatorConnection(Thread):

    _node_name = None
    _address = None
    _port = None

    _socket = None

    logger = logging.getLogger('default')

    def __init__(self, node_name, address, port):
        self._node_name = node_name
        self._address = address
        self._port = int(port)
        super(NegotiatorConnection, self).__init__()

    def reader(self):
        self.logger.debug("Start negotiator reader thread for node: '{}'".format(self._node_name))
        while event.is_set():
            try:
                message = self.rfile.readline()
            except:
                pass

    def send(self, message):
        self.wfile.write(str(message))

    def run(self):
        while event.is_set():
            try:
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.connect((self._address, self._port))
            except socket.error, msg:
                self.logger.exception("Failed connecting to negotiator on: '{}' with {}:{}".format(self._node_name, self._address, self._port))

            finally:
                time.sleep(1)


#            reader = Thread(target=self.reader)
#            reader.start()
#            reader.join()


class Negotiator(Thread):

    _config = None
    _server = None
    logger = logging.getLogger('default')

    _connections = {}

    def __init__(self, config):
        self._config = config
        super(Negotiator, self).__init__()

    def handle_message(self, event):
        self.logger.debug(event)
        pass

    def broadcast(self, message):
        pass

    def start_negotiation(self, event):
        msg = event.get_message()
        address = msg.get_src()[0]

        from pgherd.daemon import daemon
        if address in daemon.config.discoverer.local_ips:
            return

        if address in ['127.0.0.1', '0.0.0.0']:
            address = msg.get_src()[0]

        port = msg.get('port')

        self.logger.debug("Creating NegotiatorConnection to node: '{}' at: {}:{}".format(msg.get('node'), address, port))
        connection = NegotiatorConnection(msg.get('node'), address, port)
        connection.start()
        self._connections[msg.get('node')] = connection

    def stop(self):
        if self._server is not None:
            self._server.shutdown()

    def run(self):

        self.logger.info("Starting Negotiator thread")

        dispatcher.addListener('negotiator.message.receive', self.handle_message)

        SocketServer.ThreadingTCPServer.allow_reuse_address = True
        self._server = SocketServer.ThreadingTCPServer((self._config.listen, self._config.port), TcpRequestHandler)
        self._server.timeout = 3
        self.logger.info("Negotiator thread listen on {}:{}".format(self._config.listen, self._config.port))

        self._server.serve_forever()
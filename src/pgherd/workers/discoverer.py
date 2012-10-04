__author__ = 'mike'

import socket
import time
import logging
import json

from threading import Thread
from pgherd.events import dispatcher, Event
from pgherd.events import event

class Node(object):

    _name = None
    _x_log_location = 0
    _is_recovery = False
    _is_master = None

    def __init__(self, name = "", xlog_location = 0, is_recovery = False, is_master = False):
        self._name = name
        self._x_log_location = self.__xlog_to_bytes(xlog_location)
        self._is_recovery = is_recovery
        self._is_master = is_master

    def get_name(self):
        return self._name

    def is_master(self):
        return self._is_master

    def __str__(self):
        return json.dumps({'name': self._name, 'x_log_location': self._x_log_location,
                           'is_recovery': self._is_recovery, 'is_master': self._is_master})

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

class DiscovererServer(object):
    __slots__ = '__sock', '__addr', '__config', '__event'

    logger = logging.getLogger('default')

    def __init__(self, event, conf):
        self.__config = conf

        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, True)
        self.__sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        self.__sock.settimeout(0.5)
        self.__sock.bind(('0.0.0.0', self.__config.port))
        self.__addr = '255.255.255.255', self.__config.port

    def __del__(self):
        "Shutdown and close the underlying socket."
        self.__sock.shutdown(socket.SHUT_RDWR)
        self.__sock.close()

    def recv(self, size):
        "Receive a broadcast through the underlying socket."
        return self.__sock.recvfrom(size)

    def send(self, data):
        "Send a broadcast through the underlying socket."
        assert self.__sock.sendto(data, self.__addr) == len(data),\
        'Not all data was sent through the socket!'

    def __gettimeout(self):
        return self.__sock.gettimeout()

    def __settimeout(self, value):
        self.__sock.settimeout(value)

    def __deltimeout(self):
        self.__sock.setblocking(True)

    timeout = property(__gettimeout, __settimeout, __deltimeout,
        'Timeout on blocking socket operations.')

class DiscovererEvent(Event):

    def __init__(self, src, message):
        self._src = src
        self._message = message

    def __str__(self):
        return "Msg: '{}' from: {}".format(self._message, self._src)

class DiscovererListener(Thread):

    def __init__(self, conf):
        self._config = conf
        super(DiscovererListener, self).__init__()

    def run(self, server):
        while event.is_set():
            try:
                data, src = server.recv(1 << 12)
                if src[0] not in self._config.local_ips:
                    e = DiscovererEvent(src, data.decode())
                    dispatcher.notify('discoverer.broadcast.receive', e)
            except socket.timeout:
                pass


class Discoverer(Thread):

    _config = None
    _event  = None

    _listener  = None
    _server    = None

    _nodes = Nodes()

    logger = logging.getLogger('default')

    def __init__(self, conf):
        self._config = conf
        super(Discoverer, self).__init__()

    def broadcast_receive(self, event):
        print event

    def is_ready(self):
        return False

    def run(self):

        self.logger.info("Starting Discoverer thread")

        self._server = DiscovererServer(self._event, self._config)
        self._server.send("online")

        dispatcher.addListener('discoverer.broadcast.receive', self.broadcast_receive)

        self._listener = DiscovererListener(self._config)
        self._listener.run(self._server)


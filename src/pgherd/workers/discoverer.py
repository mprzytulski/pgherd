__author__ = 'mike'

import socket
import time
import logging
import json

from threading import Thread
from pgherd.events import dispatcher, Event
from pgherd.events import event

class DiscovererServer(object):
    __slots__ = '__sock', '__addr', '__config', '__event'

    logger = logging.getLogger('default')

    def __init__(self, event, conf):
        self.__config = conf

        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, True)
        self.__sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        self.__sock.settimeout(0.5)
        self.__sock.bind((self.__config.listen, self.__config.port))
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

    def get_from(self):
        return self._src

    def get_message(self):
        return self._message

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
                    dispatcher.notify('discoverer.message.receive', e)
            except socket.timeout:
                pass


class Discoverer(Thread):

    _config = None
    _event  = None

    _listener  = None
    _server    = None

    logger = logging.getLogger('default')

    def __init__(self, conf):
        self._config = conf
        super(Discoverer, self).__init__()

    def broadcast_receive(self, event):
        from pgherd.daemon import daemon
        daemon.negotiator.start_negotiation(event.get_message())
        self.logger.debug("Recive discovery request: {}".format(event))

    def is_ready(self):
        return False

    def run(self):

        self.logger.info("Starting Discoverer thread")
        from pgherd.daemon import daemon

        self._server = DiscovererServer(self._event, self._config)
        self._server.send(json.dumps({'node': daemon.node_name, 'address': daemon.node_address, 'port': self._config.port}))

        dispatcher.addListener('discoverer.message.receive', self.broadcast_receive)

        self._listener = DiscovererListener(self._config)
        self._listener.run(self._server)


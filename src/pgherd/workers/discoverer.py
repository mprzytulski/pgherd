__author__ = 'mike'

import socket
import time
import logging
import json
import SocketServer

from threading import Thread
from pgherd.events import dispatcher, Event
from pgherd.events import event


class DiscovererEvent(Event):

    def __init__(self, message):
        self._message = message

    def get_message(self):
        return self._message

    def __str__(self):
        return "DiscovererEvent.message: '{}'".format(self._message)

class DiscovererMessage(object):

    _parts = {}
    _request = None
    _socket = None

    def __init__(self, request, parts = {}, src = None, socket = None):
        self._request = request
        self._parts = parts
        self._src = src
        self._socket = socket

    def add(self, key, val):
        self._parts[key] = val

    def get(self, key, default = None):
        if self.has(key):
            return self._parts[key]
        return default

    def has(self, key):
        return key in self._parts

    def get_src(self):
        return self._src

    def reply(self, msg):
        if self._socket is None:
            return None
#            raise
        self._socket.sendto(msg, self._src)

    def __str__(self):
        x = {'request': self._request}
        x.update(self._parts)
        return json.dumps(x)

class UDPHandler(SocketServer.BaseRequestHandler):

    logger = logging.getLogger('default')

    def handle(self):
        data = json.loads(self.request[0].strip())
        request = data['request']
        del data['request']
        msg = DiscovererMessage(request, data, self.client_address, self.request[1])
        event = DiscovererEvent(msg)
        self.logger.debug("Discoverer server received: {} from: {}:{}".format(data, self.client_address[0], self.client_address[1]))
        dispatcher.notify('discoverer.message.receive.{}'.format(request), event)

class DiscovererServer(Thread):

    logger = logging.getLogger('default')

    name = 'DiscovererServer'

    def __init__(self, listen, port):
        self._listen = listen
        self._port = port
        super(DiscovererServer, self).__init__()

    def run(self):
        self.logger.info("Starting discoverer UDP server at: {}:{}".format(self._listen, self._port))
        self._server = SocketServer.ThreadingUDPServer((self._listen, self._port), UDPHandler)
        self._server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._server.serve_forever()

    def send(self, msg, address = None):
        str_data = str(msg)

        if address is None:
            address = '<broadcast>', self._port

        self.logger.debug("Publisher sending message: {} to address: {}".format(str_data, address))

        assert self._server.socket.sendto(str_data, address) == len(str_data), 'Not all data was sent through the socket!'


    def stop(self):
        if self._server is not None:
            self._server.shutdown()


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
        self.logger.debug("Broadcast message received: {}".format(event))
        from pgherd.daemon import daemon
        daemon.negotiator.start_negotiation(event)

    def master_lookup(self, event):
        from pgherd.daemon import daemon
        if daemon.node.is_master():
            reply = DiscovererMessage(daemon.node)
            event.get_src().send(reply)
        self.logger.debug("Master lookup request received")

    def is_ready(self):
        return False

    def stop(self):
        self._server.stop()

    def run(self):

        self.logger.info("Starting Discoverer thread")
        from pgherd.daemon import daemon

        dispatcher.addListener('discoverer.message.receive.node.up', self.broadcast_receive)
        dispatcher.addListener('discoverer.message.receive.master.lookup', self.master_lookup)

        self._server = DiscovererServer(self._config.listen, self._config.port)
        self._server.start()

        while not hasattr(self._server, '_server'):
            time.sleep(0.01)

        msg = DiscovererMessage('node.up')
        msg.add('node', daemon.node_fqdn)
        msg.add('address', daemon.config.daemon.listen)
        msg.add('port', daemon.config.daemon.port)

        self.logger.debug("Sending node lookup message: {}".format(str(msg)))

        self._server.send(msg)


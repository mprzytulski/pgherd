__author__ = 'mike'

from threading import Thread
from pgherdd import event
from pgherd.events import dispatcher

import logging
import pickle


import SocketServer

class TcpRequestHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        try:
            self.logger = logging.getLogger('default')
            while event.is_set():
                self.logger.debug('Wating for message')
                message = pickle.loads(self.rfile.readline().strip())
                dispatcher.notify('negotiator.message.receive', message)

        except Exception, ex:
            self.logger.exception("Unexpected error:")
        finally:
            self.request.close()

class NegotiatorMessage(object):

    _node = None

    def __init__(self, node):
        self._node = node

class NodeConnectToClusterMessage(NegotiatorMessage):
    pass

class NodeChangeStatusMessage(NegotiatorMessage):
    pass

class NodePropagateStatus(NegotiatorMessage):
    pass

class Negotiator(Thread):

    _config = None
    _event  = None
    _server = None
    logger  = None

    def __init__(self, event, config):
        self.logger = logging.getLogger('default')
        self._config = config
        self._event = event

    def handle_message(self, event):
        self.logger.debug(event)
        pass

    def run(self):

        dispatcher.addListener('negotiator.message.receive', self.handle_message)

        self._server = SocketServer.ThreadingTCPServer((self._config.listen, self._config.port), TcpRequestHandler)
        self._server.serve_forever()
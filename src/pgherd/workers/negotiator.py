__author__ = 'mike'

import logging
import json
import SocketServer

from threading import Thread
from pgherd.events import event
from pgherd.events import dispatcher

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
            self.send(NodeStatus(daemon.node))
            while event.is_set():
                self.logger.debug('Wating for message')
                message = self.read()
                dispatcher.notify('negotiator.message.receive', message)

        except Exception, ex:
            self.logger.exception("Unexpected error:")
        finally:
            self.request.close()

class NegotiatorMessage(object):

    _message = None

    def __init__(self, message):
        self._message = message

    def __str__(self):
        return json.dumps({'type': __name__, 'message': self._message.as_dict()})

class NodeConnectToClusterMessage(NegotiatorMessage):
    pass

class NodeChangeStatusMessage(NegotiatorMessage):
    pass

class NodeStatus(NegotiatorMessage):
    pass

class Negotiator(Thread):

    _config = None
    _server = None
    logger = logging.getLogger('default')

    def __init__(self, config):
        self._config = config
        super(Negotiator, self).__init__()

    def handle_message(self, event):
        self.logger.debug(event)
        pass

    def stop(self):
        self._server.shutdown()

    def run(self):

        self.logger.info("Starting Negotiator thread")

        dispatcher.addListener('negotiator.message.receive', self.handle_message)

        SocketServer.ThreadingTCPServer.allow_reuse_address = True
        self._server = SocketServer.ThreadingTCPServer((self._config.listen, self._config.port), TcpRequestHandler)
        self.logger.info("Negotiator thread listen on {}:{}".format(self._config.listen, self._config.port))

        self._server.serve_forever()
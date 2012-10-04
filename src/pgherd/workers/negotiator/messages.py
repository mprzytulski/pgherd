__author__ = 'mike'

class NegotiatorMessage(object):

    _message = None

    def __init__(self, message):
        self._message = message

    def __str__(self):
        return json.dumps({'type': type(self).__name_, 'message': self._message.as_dict()}) + "\n"

class NodeConnectToClusterMessage(NegotiatorMessage):
    pass

class NodeChangeStatusMessage(NegotiatorMessage):
    pass

class NodeStatus(NegotiatorMessage):
    pass
__author__ = 'mike'

class EventHook(object):

    def __init__(self):
        self.__handlers = []

    def __iadd__(self, handler):
        self.__handlers.append(handler)
        return self

    def __isub__(self, handler):
        self.__handlers.remove(handler)
        return self

    def fire(self, event):
        for handler in self.__handlers:
            handler(event)

    def clearObjectHandlers(self, inObject):
        for theHandler in self.__handlers:
            if theHandler.im_self == inObject:
                self -= theHandler

class Dispatcher(object):

    _listeners = {}

    def addListener(self, event, listener):
        if not self._listeners.has_key(event):
            self._listeners[event] = EventHook()
        self._listeners[event] += listener

    def notify(self, name, event):
        if self._listeners.has_key(name):
            self._listeners[name].fire(event)

class Event(object):

    pass

dispatcher = Dispatcher()
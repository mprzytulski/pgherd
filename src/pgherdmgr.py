#!/usr/bin/env python2.7
__author__ = 'mike'

import sys
import logging
import logging.handlers
import signal

from pgherd import Configuration
from pgherd.commands import Interpreter
from pgherd.events import event

command = None

def handle_event(a, b):
    print 'handle event'
    if command is not None:
        print 'command'
        if command.discoverer is not None:
            print 'discoverer'
            command.discoverer.stop()
    event.clear()

def main():

    LEVELS = { 'debug': logging.DEBUG,
               'info': logging.INFO,
               'warning': logging.WARNING,
               'error': logging.ERROR,
               'critical': logging.CRITICAL}

    logger = logging.getLogger('default')

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")


    args = sys.argv
    if type(args).__name__ == 'str':
        args = args.split(" ")

    if __file__ in args:
        args.remove(__file__)

    conf = Configuration()
    conf.parse(args, 'manager')

    # Add the log message handler to the logger
    file = logging.handlers.RotatingFileHandler(conf.logging.destination, maxBytes=10485760, backupCount=5)
    file.setFormatter(formatter)

    console = logging.StreamHandler()
    console.setFormatter(formatter)


    #    logger.addHandler(file)
    logger.addHandler(console)

    logger.setLevel(LEVELS.get('debug'))

    logger.info("pgherd staring up...")

#    event.set()

#    signal.signal(signal.SIGTERM, handle_event)
#    signal.signal(signal.SIGINT, handle_event)

    i = Interpreter(conf)

    if len(args) >= 1:
        if '-f' in args:
            args.remove(args[args.index('-f')+1])
            args.remove('-f')
        if '--config' in args:
            args.remove(args[args.index('--config')+1])
            args.remove('--config')
        if '--version' in args:
            args.remove('--version')
        i.onecmd(' '.join(args))
    else:
        i.cmdloop()




if __name__ == "__main__":
    main()

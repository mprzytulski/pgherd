__author__ = 'mike'

import sys
import daemon
import logging
import logging.handlers

from threading import Event
import signal

from pgherd import Configuration
from pgherd.workers import Monitor
from pgherd.workers import Discoverer

event = Event()

def handle_event(a, b):
    event.clear()

def main_thread(conf):

    LEVELS = { 'debug': logging.DEBUG,
               'info': logging.INFO,
               'warning': logging.WARNING,
               'error': logging.ERROR,
               'critical': logging.CRITICAL}

    logger = logging.getLogger('default')

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Add the log message handler to the logger
    file = logging.handlers.RotatingFileHandler(conf.logging.destination, maxBytes=10485760, backupCount=5)
    file.setFormatter(formatter)

    console = logging.StreamHandler()
    console.setFormatter(formatter)


#    logger.addHandler(file)
    logger.addHandler(console)

    logger.setLevel(LEVELS.get('debug'))

    logger.info("pgherd staring up...")

    try:
        event.set()

        signal.signal(signal.SIGTERM, handle_event)
        signal.signal(signal.SIGINT, handle_event)

        monitor = Monitor(event, conf.monitor)
        discoverer = Discoverer(event, conf.discoverer)

        monitor.run()
        discoverer.run()


        while event.is_set():
            logger.info('startup done. wating for signal')
            signal.pause()

    except KeyboardInterrupt:
        logger.info('^C catched - stopping server')
        raise




def main():
    conf = Configuration()
    conf.parse(sys.argv[1:])
    if conf.daemonize:
        with daemon.DaemonContext():
            main_thread(conf)
    else:
        main_thread(conf)


if __name__ == "__main__":
    main()
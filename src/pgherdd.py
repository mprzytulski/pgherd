__author__ = 'mike'

import sys
import daemon
import logging
import logging.handlers
import signal
import time

from pgherd.events import event
from pgherd import Configuration
from pgherd.workers import Monitor
from pgherd.workers import Discoverer
from pgherd.workers import Negotiator

class Daemon(object):
    monitor    = None
    discoverer = None
    negotiator = None

daemon = Daemon()


def handle_event(a, b):
    daemon.negotiator.stop()
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

        daemon.discoverer = Discoverer(event, conf.discoverer)
        daemon.negotiator = Negotiator(event, conf.daemon)
        daemon.monitor = Monitor(event, conf.monitor, daemon.discoverer)

        daemon.discoverer.start()
        daemon.negotiator.start()

        while not daemon.discoverer.is_ready():
            time.sleep(0.5)

        daemon.monitor.start()


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
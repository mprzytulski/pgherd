__author__ = 'mike'

import sys
import logging
import logging.handlers

from pgherd import Configuration
from pgherd.commands import *


def main():

    LEVELS = { 'debug': logging.DEBUG,
               'info': logging.INFO,
               'warning': logging.WARNING,
               'error': logging.ERROR,
               'critical': logging.CRITICAL}

    logger = logging.getLogger('default')

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    conf = Configuration()
    conf.parse(sys.argv[1:])

    # Add the log message handler to the logger
    file = logging.handlers.RotatingFileHandler(conf.logging.destination, maxBytes=10485760, backupCount=5)
    file.setFormatter(formatter)

    console = logging.StreamHandler()
    console.setFormatter(formatter)


    #    logger.addHandler(file)
    logger.addHandler(console)

    logger.setLevel(LEVELS.get('debug'))

    logger.info("pgherd staring up...")

    c = InitNode(conf)
    c.run()


if __name__ == "__main__":
    main()
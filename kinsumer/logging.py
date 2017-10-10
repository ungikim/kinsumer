""":mod:`linkage.helpers` --- Logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
import os
import sys
from logging import (DEBUG, Formatter, INFO,
                     StreamHandler, getLogger, getLoggerClass)
from logging.handlers import TimedRotatingFileHandler
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .consumer import Consumer

PROD_LOG_FORMAT = '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
DEBUG_LOG_FORMAT = (
    '-' * 80 + '\n' +
    '%(levelname)s in %(module)s [%(pathname)s:%(lineno)d]:\n' +
    '%(message)s\n' +
    '-' * 80
)

_Logger = getLoggerClass()


def _should_log(consumer: 'Consumer', mode: str):
    policy = consumer.config['LOGGER_HANDLER_POLICY']
    if policy == mode or policy == 'always':
        return True
    return False


def create_logger(consumer: 'Consumer') -> _Logger:  # noqa: C901
    class DebugLogger(_Logger):
        def getEffectiveLevel(self):
            if self.level == 0 and consumer.debug:
                return DEBUG
            return super().getEffectiveLevel()

    class DebugHandler(StreamHandler):
        def emit(self, record):
            if consumer.debug and _should_log(consumer, 'debug'):
                super().emit(record)

    class ProductionHandler(StreamHandler):
        def emit(self, record):
            if not consumer.debug and _should_log(consumer, 'production'):
                super().emit(record)

    debug_handler = DebugHandler()
    debug_handler.setLevel(DEBUG)
    debug_handler.setFormatter(Formatter(DEBUG_LOG_FORMAT))

    prod_handler = ProductionHandler()
    prod_handler.setLevel(INFO)
    prod_handler.setFormatter(Formatter(PROD_LOG_FORMAT))

    logger = getLogger(consumer.name)
    del logger.handlers[:]
    logger.__class__ = DebugLogger
    logger.addHandler(debug_handler)
    logger.addHandler(prod_handler)
    if (consumer.log_folder is not None and
            not hasattr(sys, '_called_from_test')):
        log_path = os.path.join(consumer.root_path, consumer.log_folder)
        if not os.path.exists(log_path):
            os.makedirs(log_path)

        log_file = os.path.join(log_path, '{0!s}.log'.format(consumer.name))

        class DebugFileHandler(TimedRotatingFileHandler):
            def emit(self, record):
                if consumer.debug and _should_log(consumer, 'debug'):
                    super().emit(record)

        class ProductionFileHandler(TimedRotatingFileHandler):
            def emit(self, record):
                if not consumer.debug and _should_log(consumer, 'production'):
                    super().emit(record)

        debug_file_handler = DebugFileHandler(
            log_file,
            when=consumer.config['LOG_ROLLOVER'],
            utc=True,
            interval=consumer.config['LOG_INTERVAL'],
            backupCount=consumer.config['LOG_BACKUP_COUNT']
        )
        debug_file_handler.setLevel(DEBUG)
        debug_file_handler.setFormatter(Formatter(DEBUG_LOG_FORMAT))

        prod_file_handler = ProductionFileHandler(
            log_file,
            when=consumer.config['LOG_ROLLOVER'],
            utc=True,
            interval=consumer.config['LOG_INTERVAL'],
            backupCount=consumer.config['LOG_BACKUP_COUNT']
        )
        prod_file_handler.setLevel(INFO)
        prod_file_handler.setFormatter(Formatter(PROD_LOG_FORMAT))
        logger.addHandler(debug_file_handler)
        logger.addHandler(prod_file_handler)
    logger.propagate = False

    return logger

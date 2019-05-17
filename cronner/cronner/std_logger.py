# -*- coding: UTF-8 -*-
"""
This simple module allows us to have a consistent logging format across services
"""
import logging


def get_logger(name, loglevel='INFO'):
    """A factory for making a logger that contains the processes name.

    :Returns: logging.LoggerAdapter

    :param name: The name of the process
    :type name: String

    :param loglevel: How verbose the logs should be
    :type loglevel: String
    """
    extra = {}
    extra['worker'] = name
    logger = logging.getLogger(name)
    logger.setLevel(loglevel.upper())
    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s [%(worker)s]: %(message)s')
        ch.setLevel(loglevel.upper())
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger = logging.LoggerAdapter(logger, extra)
    return logger

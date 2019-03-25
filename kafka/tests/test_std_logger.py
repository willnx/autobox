# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``std_logger.py`` module"""
import unittest
import logging


from log_processor import std_logger


class TestGetLogger(unittest.TestCase):
    """A suite of tests for the ``get_logger`` function"""

    def test_get_logger(self):
        """get_logger returns an instance of Pythons stdlib logging.Logger object"""
        logger = std_logger.get_logger('single_logger')
        self.assertTrue(isinstance(logger, logging.LoggerAdapter))

    def test_get_logger_one_handler(self):
        """
        We don't add a handler (causing spam multiple line outputs for a single msg)
        every time you call get_logger for the same logging object.
        """
        logger1 = std_logger.get_logger('many_loggers')
        logger2 = std_logger.get_logger('many_loggers')

        handlers = len(logger2.handlers)
        expected = 1

        self.assertEqual(handlers, expected)

    def test_get_logger_contains(self):
        """``get_logger`` contains the worker name in the log messages"""
        log = std_logger.get_logger(name='wootWorker')

        format = log.logger.handlers[0].formatter._fmt
        expected_format = '%(asctime)s [%(worker)s]: %(message)s'
        expected_extra = {'worker': 'wootWorker'}

        self.assertEqual(format, expected_format)
        self.assertEqual(log.extra, expected_extra)


if __name__ == '__main__':
    unittest.main()

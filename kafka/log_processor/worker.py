# -*- coding: UTF-8 -*-
"""Base class for creating a log processing worker

Creating a new category of log processor easy, just:

A) Subclass ``Worker``
B) Import and run ``process_logs`` from the ``manager.py` module

You'll likely want to also create a Dockerfile for the new category, update the
Makefile to have it auto-build, and update the example docker-compose.yml file.
"""
import queue
from os import environ
from abc import ABC, abstractmethod
from multiprocessing import Process

import ujson
from cryptography.fernet import Fernet, InvalidToken

from log_processor.std_logger import get_logger
from log_processor.elasticsearch import ElasticSearch

SENTINEL = 'TERMINATE YOU USELESS PROCESS'
WAIT_FOR_WORK_ITEM = 10 # seconds


class Worker(Process, ABC):
    """Carries out the processing of log data from an event queue"""
    def __init__(self, work_group, work_queue, idle_queue):
        super(Process, self).__init__()
        self.work_group = work_group
        self.work_queue = work_queue
        self.idle_queue = idle_queue
        self.keep_running = True
        self.log = get_logger(self.name)

    def run(self):
        """Defines the looping logic of the worker while it processes events"""
        self.log.info('Starting')
        while self.keep_running:
            try:
                data = self.work_queue.get(block=True)
                if data == SENTINEL:
                    self.keep_running = False
                    self.idle_queue.put((self.name, ''))
                    self.flush_on_term()
                    break
                self.process_data(data)
            except Exception as doh:
                try:
                    self.flush_on_term()
                except Exception as ugh:
                    # Flushing the buffer(s) might have had the error, plus
                    # this is just a 'best effort' to no lose data
                    self.log.exception(ugh)
                else:
                    self.log.exception(doh)
                self.keep_running = False
                self.idle_queue.put((self.name, '{}'.format(doh)))
                break

    @abstractmethod
    def process_data(self, data):
        """Defines how a specific worker should process an event off the Kafka topic"""
        raise NotImplementedError('Must define how Worker should process logs')

    @abstractmethod
    def flush_on_term(self):
        """Callback to flush any buffers before terminating."""
        raise NotImplementedError('Must define how to flush any buffers upon process scale-down')


class LogWorker(Worker):
    """Handles processing logs and then uploading data to ElasticSearch"""
    def __init__(self, work_group, work_queue, idle_queue):
        super().__init__(work_group, work_queue, idle_queue)
        server = environ['ELASTICSEARCH_SERVER']
        user = environ['ELASTICSEARCH_USER']
        doc_type = environ['ELASTICSEARCH_DOC_TYPE']
        with open(environ['ELASTICSEARCH_PASSWD_FILE'], 'rb') as pw_file:
            password = pw_file.read().strip()
        with open(environ['CIPHER_KEY_FILE'], 'rb') as cipher_file:
            self.cipher_key = cipher_file.read().strip()
        self.get_cipher()
        self.es = ElasticSearch(server, user, password, doc_type)

    def process_data(self, data):
        """Convert the log event into a JSON document, then upload to ElasticSearch"""
        try:
            info = self.extract(data)
        except (ValueError, InvalidToken) as doh:
            self.log.error('Error: {}, Data: {}'.format(doh, data))
        else:
            document = self.format_info(info)
            self.es.write(document)

    @abstractmethod
    def format_info(self, info):
        """Defines how to convert the log information into a JSON document for uploading to ElasticSearch

        :Returns: String (JSON)

        :param info: A key-value mapping of the service that create the log message, and the literal message
        :type info: Dictionary
        """
        pass

    def get_cipher(self):
        self.cipher = Fernet(self.cipher_key)

    def extract(self, data):
        """Obtain the JSON object from the encrypted data"""
        return ujson.loads(self.cipher.decrypt(data))

    def flush_on_term(self):
        """Before terminating, close the connection to ElasticSearch"""
        self.es.close()

# -*- coding: UTF-8 -*-
"""Base class for creating a log processing worker

Creating a new category of log processor easy, just:

A) Subclass ``Worker``
B) Import and run ``process_logs`` from the ``manager.py` module

You'll likely want to also create a Dockerfile for the new category, update the
Makefile to have it auto-build, and update the example docker-compose.yml file.
"""
import queue
from abc import ABC, abstractmethod
from multiprocessing import Process

from log_processor.std_logger import get_logger

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

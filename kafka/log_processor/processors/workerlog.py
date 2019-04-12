# -*- coding: UTF-8 -*-
"""Defines how to process the worker logs and upload them to ElasticSearch"""
import re
import time
from os import environ

import ujson

from log_processor.worker import LogWorker, InvalidToken
from log_processor.manager import process_logs


class WorkerLogWorker(LogWorker):
    """Defines how to parse the logs from the vLab workers.

    Yes, I know, terrible naming schema... The thing that makes the log is called
    a "worker," and the thing that processes the logs from that thing is also
    called "worker..." :(
    """
    @staticmethod
    def get_request_id(log_message):
        """Pull the client request ID from the worker log message

        Requests IDs are unique to a series of API calls. For instance, if the
        client makes several API calls to fulfill some action, the request ID
        enables us to track that distributed transaction.

        :Returns: String

        :param log_message: The full log message, including all meta-data
        :type log_message: String
        """
        # Example: f7e1bb1ccfc14954900f4b379d89301a
        the_id = re.findall('[0-9a-f]{32}', log_message)
        if len(the_id) == 1:
            return the_id[0]
        else:
            return ''

    @staticmethod
    def get_task_id(log_message):
        """Pull the task ID from the worker log message

        A task ID is unique to a specific action. For instance, creating a OneFS
        node will have a unique task id. If a client asks vLab to create 3 nodes,
        there will be 1 request ID, and 3 task IDs.

        :Returns: String

        :param log_message: The full log message, including all meta-data
        :type log_message: String
        """
        # Example: e43ed12f-621e-41f7-8117-0f4c4c400602
        the_id = re.findall('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' , log_message)
        if len(the_id) == 1:
            return the_id[0]
        else:
            return ''

    @staticmethod
    def get_message(log_message):
        """Obtain just the log message, no the log meta data

        :Returns: String

        :param log_message: The full log message, including all meta-data
        :type log_message: String
        """
        # Example
        # [2019-04-11 15:51:10,530: WARNING/ForkPoolWorker-11] 2019-04-11 15:51:10,529 [7c7a53fa69a44201acf015f5964255b1] [e43ed12f-621e-41f7-8117-0f4c4c400602]: Config OneFS 8.0.0
        # we just want the "Config OneFS 8.0.0" part
        greedy_msg = log_message.split(']')[-1]
        message = greedy_msg.replace(': ', '')
        return message

    @staticmethod
    def get_timestamp(log_message):
        """Obtain the timestamp in a format that ElasticSearch likes

        :Returns: String

        :param log_message: The full log message, including all meta-data
        :type log_message: String
        """
        # Example
        # [2019-04-11 15:51:10,530: WARNING/ForkPoolWorker-11] 2019-04-11 15:51:10,529 [7c7a53fa69a44201acf015f5964255b1] [e43ed12f-621e-41f7-8117-0f4c4c400602]: Config OneFS 8.0.0
        # We want to convert 2019-04-11 15:51:10 to 2019/04/11 15:51:10
        chunked = log_message.split(' ')
        the_date = chunked[0].replace('[', '').replace('-', '/')
        the_time = chunked[1].split(',')[0]
        timestamp = '{} {}'.format(the_date, the_time)
        return timestamp

    @staticmethod
    def task_starting(message):
        """Check if this message indicate a new task has begun.

        :Returns: Boolean

        :param message: The log message, stripped of all meta-data.
        :type message: String
        """
        if message.lower() == 'task starting\n':
            return True
        else:
            return False

    @staticmethod
    def task_competed(message):
        """Check if this message indicate a new task has finished.

        :Returns: Boolean

        :param message: The log message, stripped of all meta-data.
        :type message: String
        """
        if message.lower() == 'task complete\n':
            return True
        else:
            return False

    def format_info(self, info):
        """Convert the worker log message into a JSON object. If the log should
        be ignored, an empty string is returned.

        :Returns: String

        :param info: A key-value mapping of the service that create the log message,
                     and the literal message.
        :type info: Dictionary
        """
        request_id = self.get_request_id(info['log'])
        task_id = self.get_task_id(info['log'])
        if not task_id:
            # The Celery logs have this "double-logging" issue where every
            # event is logged twice. Disabling the generic Celery logs
            # (that lack task_ids and other meta-data) causes all worker
            # logging to be disabled...
            document = ''
        else:
            message = self.get_message(info['log'])
            timestamp = self.get_timestamp(info['log'])
            started = self.task_starting(message)
            completed = self.task_competed(message)
            formatted = {'service' : info['name'],
                         'task_id' : task_id,
                         'request_id' : request_id,
                         'started' : started,
                         'completed' : completed,
                         'message' : message,
                         'timestamp' : timestamp}
            document = ujson.dumps(formatted)
        return document


if __name__ == '__main__':
    process_logs(worker_cls=WorkerLogWorker,
                 topic='worker',
                 server=environ['KAFKA_SERVER'],
                 work_group='worker',
                 name='WorkerLogProcessor')

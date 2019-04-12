# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``workerlog`` module"""
import unittest
from unittest.mock import patch, MagicMock
import builtins
import os

from log_processor import worker
from log_processor.processors import workerlog


class TestWorkerLog(unittest.TestCase):
    """A suite of test cases for the ``WorkerLogWorker`` object"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        os.environ['CIPHER_KEY_FILE'] = './.fake_file.txt'
        os.environ['ELASTICSEARCH_SERVER'] = '127.0.0.1'
        os.environ['ELASTICSEARCH_USER'] = 'bob'
        os.environ['ELASTICSEARCH_DOC_TYPE'] = 'someLogType'
        os.environ['ELASTICSEARCH_PASSWD_FILE'] = './.fake_file.txt'

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        os.environ.pop('CIPHER_KEY_FILE', None)
        os.environ.pop('ELASTICSEARCH_SERVER', None)
        os.environ.pop('ELASTICSEARCH_USER', None)
        os.environ.pop('ELASTICSEARCH_DOC_TYPE', None)
        os.environ.pop('ELASTICSEARCH_PASSWD_FILE', None)

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_init(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``WorkerLogWorker`` accepts standard ``Worker`` init params"""
        work_group = 'web'
        work_queue = MagicMock()
        idle_queue = MagicMock()

        web_worker = workerlog.WorkerLogWorker(work_group, work_queue, idle_queue)

        self.assertTrue(isinstance(web_worker, worker.Worker))

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_format_info(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``WorkerLogWorker`` 'format_info' returns a JSON document for uploading to ElasticSearch"""
        work_group = 'web'
        work_queue = MagicMock()
        idle_queue = MagicMock()
        info = {'name' : 'vlab_cee-worker_1',
                'log' : '[2019-04-11 15:51:10,530: WARNING/ForkPoolWorker-11] 2019-04-11 15:51:10,529 [7c7a53fa69a44201acf015f5964255b1] [e43ed12f-621e-41f7-8117-0f4c4c400602]: Some Message'}
        web_worker = workerlog.WorkerLogWorker(work_group, work_queue, idle_queue)

        json_doc = web_worker.format_info(info)
        expected = '{"service":"vlab_cee-worker_1","task_id":"e43ed12f-621e-41f7-8117-0f4c4c400602","request_id":"7c7a53fa69a44201acf015f5964255b1","started":false,"completed":false,"message":"Some Message","timestamp":"2019\\/04\\/11 15:51:10"}'

        self.assertEqual(json_doc, expected)

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_format_info_junk(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``WorkerLogWorker`` 'format_info' returns an empty string if the log message lacks meta data"""
        work_group = 'web'
        work_queue = MagicMock()
        idle_queue = MagicMock()
        info = {'name' : 'vlab_cee-worker_1',
                'log' : '[2019-04-11 15:51:10,530: WARNING/ForkPoolWorker-11] Some Message'}
        web_worker = workerlog.WorkerLogWorker(work_group, work_queue, idle_queue)

        json_doc = web_worker.format_info(info)
        expected = ''

        self.assertEqual(json_doc, expected)

    def test_get_request_id(self):
        """``WorkerLogWorker`` extracts the client request ID from a worker log message"""
        log_message = '[2019-04-11 15:51:10,530: WARNING/ForkPoolWorker-11] 2019-04-11 15:51:10,529 [7c7a53fa69a44201acf015f5964255b1] [e43ed12f-621e-41f7-8117-0f4c4c400602]: Some Message'

        request_id = workerlog.WorkerLogWorker.get_request_id(log_message)
        expected = '7c7a53fa69a44201acf015f5964255b1'

        self.assertEqual(request_id, expected)

    def test_get_request_id_missing(self):
        """``WorkerLogWorker`` returns an empty string if no request id is in the message"""
        log_message = 'some crazy message'

        request_id = workerlog.WorkerLogWorker.get_request_id(log_message)
        expected = ''

        self.assertEqual(request_id, expected)

    def test_get_task_id(self):
        """``WorkerLogWorker`` extracts the task ID from a worker log message"""
        log_message = '[2019-04-11 15:51:10,530: WARNING/ForkPoolWorker-11] 2019-04-11 15:51:10,529 [7c7a53fa69a44201acf015f5964255b1] [e43ed12f-621e-41f7-8117-0f4c4c400602]: Some Message'

        task_id = workerlog.WorkerLogWorker.get_task_id(log_message)
        expected = 'e43ed12f-621e-41f7-8117-0f4c4c400602'

        self.assertEqual(task_id, expected)

    def test_get_task_id_missing(self):
        """``WorkerLogWorker`` returns an empty string if no task id is in the message"""
        log_message = 'some other crazy message'

        task_id = workerlog.WorkerLogWorker.get_task_id(log_message)
        expected = ''

        self.assertEqual(task_id, expected)

    def test_get_message(self):
        """``WorkerLogWorker`` extracts the actual message from a worker log message"""
        log_message = '[2019-04-11 15:51:10,530: WARNING/ForkPoolWorker-11] 2019-04-11 15:51:10,529 [7c7a53fa69a44201acf015f5964255b1] [e43ed12f-621e-41f7-8117-0f4c4c400602]: Some Message'

        message = workerlog.WorkerLogWorker.get_message(log_message)
        expected = 'Some Message'

        self.assertEqual(message, expected)

    def test_get_timestamp(self):
        """``WorkerLogWorker`` extracts an ElasticSearch friendly timestamp from a worker log message"""
        log_message = '[2019-04-11 15:51:10,530: WARNING/ForkPoolWorker-11] 2019-04-11 15:51:10,529 [7c7a53fa69a44201acf015f5964255b1] [e43ed12f-621e-41f7-8117-0f4c4c400602]: Some Message'

        timestamp = workerlog.WorkerLogWorker.get_timestamp(log_message)
        expected = '2019/04/11 15:51:10'

        self.assertEqual(timestamp, expected)

    def test_task_starting(self):
        """``WorkerLogWorker`` can detect if a message is the start of a task"""
        message = 'Task Starting\n'

        starting = workerlog.WorkerLogWorker.task_starting(message)
        expected = True

        self.assertEqual(starting, expected)

    def test_task_not_starting(self):
        """``WorkerLogWorker`` can detect if a message is not the start of a task"""
        message = 'Some Message'

        starting = workerlog.WorkerLogWorker.task_starting(message)
        expected = False

        self.assertEqual(starting, expected)

    def test_task_completed(self):
        """``WorkerLogWorker`` can detect if a message is the completion of a task"""
        message = 'Task Complete\n'

        starting = workerlog.WorkerLogWorker.task_competed(message)
        expected = True

        self.assertEqual(starting, expected)

    def test_task_not_completed(self):
        """``WorkerLogWorker`` can detect if a message is not the completion of a task"""
        message = 'Some Message'

        starting = workerlog.WorkerLogWorker.task_competed(message)
        expected = False

        self.assertEqual(starting, expected)




if __name__ == '__main__':
    unittest.main()

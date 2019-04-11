# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``worker.py`` module"""
import unittest
from unittest.mock import patch, MagicMock
import time
import builtins
import queue
from multiprocessing import Queue
import os

from log_processor import worker


class DerpWorker(worker.Worker):
    """Exists solely to test the ``Worker`` abstract base class"""
    def process_data(self, data):
        return True
    def flush_on_term(self):
        pass


class DerpLogWorker(worker.LogWorker):
    """Exists solely to test the ``LogWorker`` abstract base class"""
    def format_info(self, data):
        return '{"worked":true}'


class TestWorker(unittest.TestCase):
    """A suite of test cases for the ``Worker`` object"""
    def test_abstract_methods(self):
        try:
            worker.Worker()
        except Exception as doh:
            error = '{}'.format(doh)
        else:
            error = 'ERROR: did not detected abstract methods'
        expected = "Can't instantiate abstract class Worker with abstract methods flush_on_term, process_data"

        self.assertEqual(error, expected)

    def test_init_params(self):
        """``Worker`` requires init params 'work_group', 'work_queue', and 'idle_queue'"""
        try:
            DerpWorker(work_group='testing', work_queue=MagicMock(), idle_queue=MagicMock())
        except Exception:
            passed = False
        else:
            passed = True

        self.assertTrue(passed)

    def test_process_data(self):
        """``Worker`` method 'process_data' requires param 'data'"""
        w = DerpWorker(work_group='testing', work_queue=MagicMock(), idle_queue=MagicMock())
        self.assertTrue(w.process_data('someData'))

    def test_flush_on_term(self):
        """``Worker`` method 'flush_on_term' takes no params"""
        w = DerpWorker(work_group='testing', work_queue=MagicMock(), idle_queue=MagicMock())
        resp = w.flush_on_term()
        expected = None

        self.assertEqual(resp, expected)

    @patch.object(worker, 'get_logger')
    @patch.object(DerpWorker, 'process_data')
    def test_run_processes_data(self, fake_process_data, fake_get_logger):
        """``Worker`` the 'run' method calls 'process_data' with what is pulls form the work_queue"""
        idle_queue = MagicMock()
        work_queue = Queue()
        work_queue.put('some Work')
        work_queue.put(worker.SENTINEL)

        w = DerpWorker(work_group='testing', work_queue=work_queue, idle_queue=idle_queue)
        w.run()
        work_queue.close()
        work_queue.join_thread()

        called_process_data = fake_process_data.called

        self.assertTrue(called_process_data)

    @patch.object(DerpWorker, 'flush_on_term')
    @patch.object(worker, 'get_logger')
    @patch.object(DerpWorker, 'process_data')
    def test_run_exception(self, fake_process_data, fake_get_logger, fake_flush_on_term):
        """``Worker`` the 'run' method calls 'flush_on_term' if 'process_data' raises an Exception"""
        idle_queue = MagicMock()
        work_queue = Queue()
        fake_process_data.side_effect = Exception('testing')
        work_queue.put('some Work')

        w = DerpWorker(work_group='testing', work_queue=work_queue, idle_queue=idle_queue)
        w.run()
        work_queue.close()
        work_queue.join_thread()

        called_fake_flush_on_term = fake_flush_on_term.called

        self.assertTrue(called_fake_flush_on_term)

    @patch.object(DerpWorker, 'flush_on_term')
    @patch.object(worker, 'get_logger')
    @patch.object(DerpWorker, 'process_data')
    def test_run_flush_retire(self, fake_process_data, fake_get_logger, fake_flush_on_term):
        """``Worker`` 'run' flushes any data before retiring"""
        idle_queue = MagicMock()
        work_queue = Queue()
        work_queue.put(worker.SENTINEL)

        w = DerpWorker(work_group='testing', work_queue=work_queue, idle_queue=idle_queue)
        w.run()
        work_queue.close()
        work_queue.join_thread()

        called_fake_flush_on_term = fake_flush_on_term.called

        self.assertTrue(called_fake_flush_on_term)

    @patch.object(worker, 'get_logger')
    @patch.object(DerpWorker, 'flush_on_term')
    @patch.object(DerpWorker, 'process_data')
    def test_run_flush_exception(self, fake_process_data, fake_flush_on_term, fake_logger):
        """``Worker`` an exception caught while flushing buffered data still notifies the idle_queue"""
        idle_queue = MagicMock()
        work_queue = Queue()
        work_queue.put(worker.SENTINEL)
        fake_flush_on_term.side_effect = Exception('testing')

        w = DerpWorker(work_group='testing', work_queue=work_queue, idle_queue=idle_queue)
        w.run()
        work_queue.close()
        work_queue.join_thread()

        singled_manager = idle_queue.put.called
        expected = True

        self.assertEqual(singled_manager, expected)


class TestLogWorker(unittest.TestCase):
    """A suite of test cases for the ``LogWorker`` object"""
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
        """``LogWorker`` accepts the standard INIT params as any other worker"""
        work_group = 'someLogProcessor'
        work_queue = MagicMock()
        idle_queue = MagicMock()

        log_worker = DerpLogWorker(work_group, work_queue, idle_queue)

        self.assertTrue(isinstance(log_worker, worker.LogWorker))

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_extract(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``LogWorker`` the 'extract' method decrypts and parses the JSON into a usable object"""
        fake_cipher = MagicMock()
        fake_cipher.decrypt.return_value = '{"worked":true}'
        fake_Fernet.return_value = fake_cipher
        work_group = 'someLogProcessor'
        work_queue = MagicMock()
        idle_queue = MagicMock()

        log_worker = DerpLogWorker(work_group, work_queue, idle_queue)
        data = log_worker.extract('some encrypted data')
        expected = {"worked" : True}

        self.assertEqual(data, expected)

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_flush_on_term(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``LogWorker`` the 'flush_on_term' method closes the TCP socket with the ElasticSearch server"""
        fake_es = MagicMock()
        fake_ElasticSearch.return_value = fake_es
        work_group = 'someLogProcessor'
        work_queue = MagicMock()
        idle_queue = MagicMock()

        log_worker = DerpLogWorker(work_group, work_queue, idle_queue)
        log_worker.flush_on_term()

        self.assertTrue(fake_es.close.called)




if __name__ == "__main__":
    unittest.main()

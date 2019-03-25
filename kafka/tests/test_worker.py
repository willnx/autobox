# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``worker.py`` module"""
import unittest
from unittest.mock import patch, MagicMock
import time
import queue
from multiprocessing import Queue

from log_processor import worker


class DerpWorker(worker.Worker):
    """Exists solely to test the ``Worker`` abstract base class"""
    def process_data(self, data):
        return True
    def flush_on_term(self):
        pass


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


if __name__ == "__main__":
    unittest.main()
